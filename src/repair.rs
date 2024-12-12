use crate::delta_helpers::load_table;
use deltalake_core::{DeltaOps, DeltaTableError, PartitionFilter};
use log::{error, info};
use std::collections::HashMap;
use std::convert::TryFrom;

/// Repairs and Optimizes a Delta table. This function will scan the table and repair any issues it finds. Then it will execute an optimize operation on the table.
/// If a predicate is provided, the optimize operation will only optimize files that match the partition predicate.
///
pub async fn repair_and_optimize_table(
    table_url: String,
    optimize_target_size: usize,
    optimize_predicate: Option<(String, String)>,
) -> Result<(), DeltaTableError> {
    info!("Executing repair on table: {}", table_url);
    let table = load_table(table_url.as_str(), HashMap::new()).await;
    match table {
        Ok(table) => {
            let op = DeltaOps::from(table);
            let (table, metrics) = op.filesystem_check().await?;
            info!("Table repaired: {:?}", &table);
            info!("Metrics: {:?}", metrics);
            let mut filter = vec![];
            let delta_op = match optimize_predicate {
                Some((partition, predicate)) => {
                    filter.push(PartitionFilter::try_from((
                        partition.as_str(),
                        "=",
                        predicate.as_str(),
                    ))?);
                    info!("Partition filter: {:?}", filter);
                    DeltaOps(table).optimize().with_filters(&filter)
                }
                None => DeltaOps(table).optimize(),
            };
            match delta_op.with_target_size(optimize_target_size as i64).await {
                Ok((_dt, metrics)) => {
                    info!("Table optimized");
                    info!("Metrics: {:?}", metrics);
                    Ok(())
                }
                Err(e) => {
                    error!("Error optimizing table: {:?}", e);
                    Err(e)
                }
            }
        }
        Err(e) => {
            error!("Error loading table: {:?}", e);
            Err(e)
        }
    }
}
