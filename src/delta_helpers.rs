use crate::{DataTypeOffset, DataTypePartition};
use deltalake_core::kernel::{Action, Add, Transaction};
use deltalake_core::{DeltaTable, DeltaTableError};
use log::{trace, warn};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::Mutex;

pub(crate) async fn load_table(
    table_uri: &str,
    options: HashMap<String, String>,
) -> Result<DeltaTable, DeltaTableError> {
    let mut table = deltalake_core::open_table_with_storage_options(table_uri, options).await?;
    table.load().await?;
    Ok(table)
}

pub(crate) fn build_actions(
    partition_offsets: &HashMap<DataTypePartition, DataTypeOffset>,
    app_id: &str,
    mut add: Vec<Add>,
) -> Vec<Action> {
    partition_offsets
        .iter()
        .map(|(partition, offset)| {
            create_txn_action(txn_app_id_for_partition(app_id, *partition), *offset)
        })
        .chain(add.drain(..).map(Action::Add))
        .collect()
}

pub(crate) fn create_txn_action(txn_app_id: String, offset: DataTypeOffset) -> Action {
    Action::Txn(Transaction {
        app_id: txn_app_id,
        version: offset,
        last_updated: Some(
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_millis() as i64,
        ),
    })
}

pub(crate) async fn try_create_checkpoint(
    table: &mut DeltaTable,
    version: i64,
    snapshot_mutex: Option<Arc<Mutex<()>>>,
) -> Result<(), DeltaTableError> {
    if version % 10 == 0 {
        let table_version = table.version();
        // if there's new version right after current commit, then we need to reset
        // the table right back to version to create the checkpoint
        let version_updated = table_version != version;
        if version_updated {
            table.load_version(version).await?;
        }
        // create checkpoint is memory intensive. we lock here to ensure we only try to create one checkpoint at a time.
        let mut aquired_lock = None;
        if let Some(lock) = snapshot_mutex.as_ref() {
            aquired_lock = Some(lock.lock().await);
        }
        deltalake_core::checkpoints::create_checkpoint(table).await?;
        log::info!("Created checkpoint version {}.", version);

        let removed = deltalake_core::checkpoints::cleanup_metadata(table).await?;
        if removed > 0 {
            log::info!("Metadata cleanup, removed {} obsolete logs.", removed);
        }
        if aquired_lock.is_some() {
            drop(aquired_lock);
        }
        if version_updated {
            table.update().await?;
        }
    }
    Ok(())
}

pub(crate) fn txn_app_id_for_partition(app_id: &str, partition: DataTypePartition) -> String {
    format!("{}-{}", app_id, partition)
}

/// Returns the last transaction version for the given transaction id recorded in the delta table. Will Seek Back N Transactions to find the given app_id.
pub(crate) async fn last_txn_version(
    table: &mut DeltaTable,
    app_id: &str,
    table_name: &str,
    max_version_lookback: usize,
) -> Option<i64> {
    let mut latest = table
        .get_app_transaction_version()
        .get(app_id)
        .map(|t| t.version);
    let latest_version = table.version();
    loop {
        if latest.is_some() {
            break;
        }
        let v = table.version();
        if v == 0 || latest_version - v >= max_version_lookback as i64 {
            trace!(
                "Exhausted version look-back. Aborting for [{}] with app_id [{}]",
                table_name,
                app_id
            );
            break;
        }
        trace!(
            "Loading Prior version of table [{}] to find partition for appid [{}].",
            table_name,
            app_id
        );
        table
            .load_version(v - 1)
            .await
            .expect("Failed to load prior version of table.");
        //table.get_app_transaction_version().keys().for_each(|e| info!("Found Key [{}] on version [{}] of table [{}]",e,v,table_name));
        latest = table
            .get_app_transaction_version()
            .get(app_id)
            .map(|t| t.version);
    }
    if table.version() != latest_version {
        trace!(
            "Restoring latest version of table [{}] after we found offset for partition [{}].",
            table_name,
            app_id
        );
        table
            .load_version(latest_version)
            .await
            .expect("Failed to load current version of table.");
    }
    if latest.is_none() {
        warn!(
            "Failed to find a value for Partition in the given table [{}] for appid [{}]",
            table_name, app_id
        );
    }
    latest
}
