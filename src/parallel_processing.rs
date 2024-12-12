#[cfg(feature = "akunaformats")]
use akuna_deserializer::protocol_decoder::remove_map_marker_entries;
use deltalake_core::protocol::{DeltaOperation, OutputMode};
use deltalake_core::{DeltaTable, DeltaTableError};
use log::{debug, error, info, trace, warn};
use rdkafka::consumer::{CommitMode, Consumer, StreamConsumer};
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::Instant;

use rdkafka::message::BorrowedMessage;
use rdkafka::{Message, Offset};
use regex::Regex;
use serde_json::Value;
use tokio::spawn;
use tokio::sync::mpsc::{Receiver, Sender};
use tokio::sync::{mpsc, Mutex, RwLock};

use crate::coercions::CoercionTree;
use crate::dead_letters::{DeadLetter, DeadLetterQueue};
use crate::delta_helpers::{
    build_actions, last_txn_version, load_table, try_create_checkpoint, txn_app_id_for_partition,
};
use crate::metrics::{IngestMetrics, StatType};
use crate::serialization::{MessageDeserializer, MessageType};
use crate::transforms::Transformer;
use crate::value_buffers::{ConsumedBuffers, ValueBuffers};
use crate::writer::{DataWriter, DataWriterError};
use crate::{
    build_metrics, coercions, create_columns, dead_letter_queue_from_options,
    kafka_client_config_from_options, read_entries_from_s3, AutoOffsetReset, DataTypeOffset,
    DataTypePartition, IngestError, IngestOptions, KafkaContext, MessageDeserializationError,
    PartitionAssignment, RebalanceAction, RebalanceSignal, DEFAULT_DELTA_MAX_RETRY_COMMIT_ATTEMPTS,
};
use async_channel::unbounded;
use core::time::Duration;
use deltalake_core::operations::transaction::TableReference;
use futures::executor::block_on;
use rdkafka::error::KafkaError;
use rdkafka::util::Timeout;
use serde_json::Value::Null;
use std::cmp::min;
use std::future::Future;
use std::ops::Add;
use tokio::sync::mpsc::error::SendError;
use tokio::task::{spawn_blocking, JoinHandle};
use tokio_util::sync::CancellationToken;

const MAX_KAFKA_RECEIVE_FAIL_COUNT: usize = 10;

enum ActorMessageTypes {
    MessageToDeserialize(BorrowedMessage<'static>),
    NewDeserializedMessage(BorrowedMessage<'static>, MessageType, Value),
    DeserializeFailedMessage(
        BorrowedMessage<'static>,
        Option<MessageDeserializationError>,
    ),
    FailedTransformRecord(DataTypePartition, DataTypeOffset, Value),
    DeltaRecordBatchSet(usize, Receiver<ActorMessageTypes>),
    NewDeltaRecord(DataTypePartition, DataTypeOffset, Value),
    Rebalance(HashSet<DataTypePartition>, HashSet<DataTypePartition>), // adds, removes
    DeltaTableState(String, HashMap<DataTypePartition, DataTypeOffset>), // For a given Message Type what is the last offsets written
    DLQ(DeadLetter),
    Terminate,
    Flush,
}

pub struct KafkaReadingActor {
    message_type_whitelist: Option<Regex>,
    topic: String,
    table_base_uri: String,
    opts: IngestOptions,
    consumer: Arc<StreamConsumer<KafkaContext>>,
    message_deserializer: &'static (dyn MessageDeserializer + Send + Sync),
    pub rebalance_signal: Arc<RwLock<Option<RebalanceSignal>>>,
    pub cancel_token: Option<Arc<CancellationToken>>,
    worker_to_messages_multiplier: usize,
}

impl KafkaReadingActor {
    pub fn new(
        topic: String,
        table_base_uri: String,
        opts: IngestOptions,
        message_deserializer: Box<dyn MessageDeserializer + Send + Sync>,
        cancel_token: Option<Arc<CancellationToken>>,
    ) -> Self {
        // Initialize a RebalanceSignal to share between threads so it can be set when rebalance events are sent from Kafka and checked or cleared in the run loop.
        // We use an RwLock so we can quickly skip past the typical case in the run loop where the rebalance signal is a None without starving the writer.
        // See also `handle_rebalance`.
        let rebalance_signal = Arc::new(RwLock::new(None));

        // compile the regex for message type whitelist
        let message_type_whitelist = match &opts.message_type_whitelist {
            Some(s) => Some(Regex::new(&s).unwrap()),
            None => None,
        };
        let kafka_consumer_context = KafkaContext {
            rebalance_signal: rebalance_signal.clone(),
        };
        let kafka_client_config = kafka_client_config_from_options(&opts);
        let consumer: StreamConsumer<KafkaContext> = kafka_client_config
            .create_with_context(kafka_consumer_context)
            .expect("Failed to create Kafka consumer");
        let consumer = Arc::new(consumer);
        let worker_to_messages_multiplier = opts.worker_to_messages_multiplier;
        let message_deserializer = Box::leak(message_deserializer);
        Self {
            message_type_whitelist,
            topic,
            table_base_uri,
            opts,
            consumer,
            message_deserializer,
            rebalance_signal,
            cancel_token,
            worker_to_messages_multiplier,
        }
    }

    pub async fn start(&'static mut self) -> Result<(), IngestError> {
        info!(
            "Ingesting messages from [{}] Kafka topic to [{}] Delta tables located at base",
            self.topic, self.table_base_uri
        );
        let message_queue_default_size =
            self.opts.deserialization_workers * self.worker_to_messages_multiplier * 4;
        let housekeeping_loop_iteration = self.opts.housekeeping_loop_iteration;
        let transformer = Transformer::from_transforms(&self.opts.transforms)?;
        let mut types_to_sink: HashMap<MessageType, Sender<ActorMessageTypes>> = HashMap::new();
        let mut ingest_metrics = build_metrics(&self.opts)?;
        let mut partition_assignment = PartitionAssignment::default();
        let (dlq_sink, dlq_tx) = DLQSink::new(
            dead_letter_queue_from_options(&self.opts).await?,
            message_queue_default_size,
        )
        .await;
        spawn(dlq_sink.start());
        let dlq_tx = Box::leak(Box::new(dlq_tx));

        // setup the channel for the table actors to communicate back the offsets.
        let (tx_delta_table_state, mut rx_delta_table_state) =
            mpsc::channel(message_queue_default_size);

        // a mutex to be passed to delta writers. we want to ensure onlu 1 checkpoint create at a time to prevent memory spikes.
        let checkpoint_mutex = Arc::new(tokio::sync::Mutex::new(()));

        // Read a list of entries from an S3 location:
        self.setup_pre_existing_tables(
            &transformer,
            &mut types_to_sink,
            dlq_tx,
            &tx_delta_table_state,
            checkpoint_mutex.clone(),
        )
        .await?;

        // setup the pool of deserializing workers
        let (tx_raw_message, rx_raw_message) = unbounded::<ActorMessageTypes>();

        let mut rx_deserialized =
            self.setup_deserialization_actors(rx_raw_message, message_queue_default_size);

        let idle_timer = Duration::from_secs(self.opts.seconds_idle_kafka_read_before_flush as u64);

        self.consumer
            .subscribe(&[self.topic.as_str()])
            .expect("Failed to subscribe to Kafka topic");
        let mut loop_count = 0usize;
        info!("Starting the Main Ingest Loop");
        let mut last_flush_time = Instant::now();
        loop {
            trace!("Checking if we need to Re-balance");
            let should_reset_offsets = self
                .handle_rebalance(
                    self.rebalance_signal.clone(),
                    &types_to_sink,
                    &mut partition_assignment,
                )
                .await?;
            if should_reset_offsets {
                let offsets_to_seek = Self::get_latest_offsets_from_delta_tables(
                    &mut types_to_sink,
                    &mut rx_delta_table_state,
                )
                .await;
                partition_assignment.update_offsets(&offsets_to_seek);
                self.seek_consumer(&partition_assignment)?;
                last_flush_time = Instant::now();
            } else {
                // check if we need to flush. need to do this in case some message type only comes
                // infrequently on this partition so we dont buffer this indefinately.
                if last_flush_time.elapsed().as_secs() > self.opts.allowed_latency {
                    debug!("Flushing All the tables");
                    for c in types_to_sink.values() {
                        c.send(ActorMessageTypes::Flush)
                            .await
                            .expect("Failed to send flush message to handling actor");
                    }
                    last_flush_time = Instant::now();
                }
            }
            trace!("Polling for messages");
            let messages = self.get_messages_from_receiver(&types_to_sink).await?;
            if loop_count % housekeeping_loop_iteration == 0 {
                debug!("Committing Kafka Messages");
                // find the last offset message per partition and commit it.
                let mut last_offsets = HashMap::new();
                for msg in messages.iter().as_ref().iter().rev() {
                    // add the last offset for each partition
                    let highest_offset =
                        last_offsets.entry(msg.partition()).or_insert(msg.offset());
                    if *highest_offset >= msg.offset() {
                        self.consumer
                            .commit_message(msg, CommitMode::Async)
                            .expect("Failed to commit offset back to Kafka.");
                    }
                }
            }
            trace!("Deserializing Messages");
            let start_of_deserialization = Instant::now();
            let received_kafka_msg_count = messages.len();
            for msg in messages.into_iter() {
                tx_raw_message
                    .send(ActorMessageTypes::MessageToDeserialize(msg))
                    .await
                    .expect("Failed to send to deserializer");
            }
            trace!("Waiting for Deserialized Messages");
            let mut deserialized_messages: Vec<(
                Result<(MessageType, Value), MessageDeserializationError>,
                BorrowedMessage,
            )> = Vec::with_capacity(received_kafka_msg_count);
            for _i in 0..received_kafka_msg_count {
                match tokio::time::timeout(idle_timer, rx_deserialized.recv()).await {
                    Ok(Some(ActorMessageTypes::NewDeserializedMessage(
                        msg,
                        message_type,
                        value,
                    ))) => {
                        //ingest_metrics.message_deserialized(message_type.as_str());
                        deserialized_messages.push((Ok((message_type, value)), msg));
                    }
                    Ok(Some(ActorMessageTypes::DeserializeFailedMessage(msg, e))) => {
                        ingest_metrics.message_deserialization_failed();
                        deserialized_messages.push((Err(e.unwrap()), msg));
                    }
                    Err(timeout_error) => {
                        error!(
                            "Timeout after {:?} while waiting to receive a message from deserialisation actors.",
                            timeout_error
                        );
                        return Err(IngestError::SendError {});
                    }
                    _ => {
                        warn!("Received unexpected message from deserializer");
                    }
                }
            }
            ingest_metrics.deserialize_completed(&start_of_deserialization);

            trace!("Setting up new tables");
            self.setup_new_tables(
                transformer.clone(),
                &mut types_to_sink,
                dlq_tx,
                tx_delta_table_state.clone(),
                &mut deserialized_messages,
                checkpoint_mutex.clone(),
            )
            .await?;

            trace!("Separate out the messages by message type. This is so we can send them to the correct actor");
            let mut message_type_counts = HashMap::with_capacity(types_to_sink.len());

            let mut message_sending_futures = Vec::new();

            let msgs_by_type: HashMap<MessageType, Vec<ActorMessageTypes>> = deserialized_messages
                .into_iter()
                .filter_map(|(result, msg)| {
                    match result {
                        Ok((message_type, value)) => {
                            // let count = message_type_counts.entry(message_type.clone()).or_insert(0);
                            let _entry = message_type_counts
                                .entry(message_type.clone())
                                .and_modify(|e| *e += 1)
                                .or_insert(1);
                            //*entry += 1;
                            // check whitelist if we want to process this message
                            if let Some(whitelist) = &self.message_type_whitelist {
                                if !whitelist.is_match(message_type.as_str()) {
                                    return None;
                                }
                            }
                            Some((
                                message_type.clone(),
                                ActorMessageTypes::NewDeserializedMessage(msg, message_type, value),
                            ))
                        }
                        Err(e) => {
                            message_sending_futures.push(dlq_tx.send(ActorMessageTypes::DLQ(
                                DeadLetter::from_failed_deserialization(
                                    msg.payload().unwrap(),
                                    e.to_string(),
                                ),
                            )));
                            None
                        }
                    }
                })
                .fold(HashMap::new(), |mut acc, e| {
                    acc.entry(e.0).or_insert_with(Vec::new).push(e.1);
                    acc
                });
            for (message_type, count) in message_type_counts.iter() {
                trace!("Message Type: [{}] Count: [{}]", message_type, count);
                ingest_metrics.n_message_deserialized(message_type.as_str(), *count as i64);
            }

            if message_sending_futures.len() > 0 {
                trace!(
                    "Waiting for Message Send Completion on [{}]",
                    message_sending_futures.len()
                );
                Self::ensure_send_completion(message_sending_futures).await?;
            }
            // now loop over the messages by bucket and send them to the correct actor.
            trace!("Sending Messages to Actors");
            let mut tx_vec = Vec::with_capacity(msgs_by_type.len());
            for (msg_type, msgs) in msgs_by_type.into_iter() {
                // setup the channel and get the message count to create a batch message:
                let count = msgs.len();
                let (tx, rx) = mpsc::channel(count);
                tx_vec.push(tx);
                let batch = ActorMessageTypes::DeltaRecordBatchSet(count, rx);
                match &types_to_sink.get(&msg_type) {
                    None => {
                        error!(
                            "No Actor for message type: [{}]. This should not happen.",
                            msg_type
                        );
                        continue;
                    }
                    Some(a) => {
                        a.send(batch)
                            .await
                            .expect("Failed to send to handling actor");
                    }
                }
                let mut message_sending_futures = Vec::with_capacity(msgs.len());
                for msg in msgs {
                    message_sending_futures.push(tx_vec.last().unwrap().send(msg));
                }
                trace!(
                    "Waiting for Message Send Completion for [{}] with [{}] messages.",
                    msg_type,
                    message_sending_futures.len()
                );
                Self::ensure_send_completion(message_sending_futures).await?;
            }
            loop_count = loop_count + 1;
            if loop_count % housekeeping_loop_iteration == 0 {
                debug!("Wrapping up Loop iteration [{}]", loop_count);
                dlq_tx
                    .send(ActorMessageTypes::Flush)
                    .await
                    .expect("Failed to contact the DLQ Actor.");
            }
            if let Some(ct) = self.cancel_token.as_ref() {
                if ct.is_cancelled() {
                    return Ok(());
                }
            }
        }
    }

    // inline this function
    #[inline]
    #[allow(dead_code)]
    async fn ensure_send_completion_seq(
        message_sending_futures: Vec<
            impl Future<Output = Result<(), SendError<ActorMessageTypes>>> + Sized,
        >,
    ) -> Result<(), IngestError> {
        trace!(
            "Joining the Send Futures in a sequential fashion to ensure sequencing is preserved."
        );
        for f in message_sending_futures {
            if let Err(e) = f.await {
                error!("Error sending message to handling actor. Error: {:?}", e);
                return Err(IngestError::SendError {});
            }
        }

        Ok(())
    }

    async fn ensure_send_completion(
        message_sending_futures: Vec<
            impl Future<Output = Result<(), SendError<ActorMessageTypes>>> + Sized,
        >,
    ) -> Result<(), IngestError> {
        trace!("Joining the Send Futures");
        if futures::future::join_all(message_sending_futures)
            .await
            .into_iter()
            .filter_map(|e| {
                if e.is_err() {
                    let err = e.err().unwrap();
                    error!("Error sending message to handling actor. Error: {:?}", err);
                    Some(err)
                } else {
                    None
                }
            })
            .collect::<Vec<_>>()
            .len()
            > 0
        {
            return Err(IngestError::SendError {}); // if we had a problem sending we need to abort.
        }

        Ok(())
    }

    async fn setup_new_tables(
        &self,
        transformer: Transformer,
        types_to_sink: &mut HashMap<MessageType, Sender<ActorMessageTypes>>,
        dlq_tx: &mut Sender<ActorMessageTypes>,
        tx_delta_table_state: Sender<ActorMessageTypes>,
        deserialized_messages: &mut Vec<(
            Result<(MessageType, Value), MessageDeserializationError>,
            BorrowedMessage<'_>,
        )>,
        checkpoint_mutex: Arc<Mutex<()>>,
    ) -> Result<(), IngestError> {
        let existing_message_types: &HashSet<MessageType> =
            &types_to_sink.keys().cloned().collect(); // TODO: Can we get rid of this clone?
        trace!(
            "Checking for new Message Types {:?} in Batch",
            existing_message_types
        );
        let new_message_types_in_batch: HashMap<MessageType, (Value, &BorrowedMessage)> =
            deserialized_messages.iter().filter(|e| e.0.is_ok()).fold(
                HashMap::new(),
                |mut acc, (e, m)| {
                    match e {
                        Err(_) => {}
                        Ok(e) => {
                            if !existing_message_types.contains(&e.0) && !acc.contains_key(&e.0) {
                                acc.insert(e.0.clone(), (e.1.clone(), m));
                            }
                        }
                    };
                    acc
                },
            );
        {
            let local_types_to_sink = types_to_sink;
            for mut message_type_value_tuple in new_message_types_in_batch.into_iter() {
                // checking the whitelist and skipping if we dont need this type.
                if let Some(whitelist) = &self.message_type_whitelist {
                    if !whitelist.is_match(message_type_value_tuple.0.as_str()) {
                        continue;
                    }
                }
                // applying transforms
                transformer
                    .transform(
                        &mut message_type_value_tuple.1 .0,
                        Some(message_type_value_tuple.1 .1),
                    )
                    .expect("Failed to apply transforms");
                info!("Creating Table for [{}]", message_type_value_tuple.0);
                self.create_new_delta_table(
                    &message_type_value_tuple.0,
                    &message_type_value_tuple.1 .0,
                )
                .await
                .expect("Could not create new table. We must now shut down.");
                let new_mca_tx = self
                    .setup_new_message_type_table_sink_actor(
                        dlq_tx.clone(),
                        tx_delta_table_state.clone(),
                        &message_type_value_tuple.0.to_string(),
                        transformer.clone(),
                        build_metrics(&self.opts)?,
                        checkpoint_mutex.clone(),
                    )
                    .await
                    .expect("Could not create new table. We must now shut down.");
                local_types_to_sink.insert(message_type_value_tuple.0, new_mca_tx);
            }
        };
        Ok(())
    }

    async fn get_latest_offsets_from_delta_tables(
        types_to_sink: &mut HashMap<MessageType, Sender<ActorMessageTypes>>,
        rx_delta_table_state: &mut Receiver<ActorMessageTypes>,
    ) -> HashMap<DataTypePartition, DataTypeOffset> {
        let mut currently_sinked_offsets = HashMap::with_capacity(types_to_sink.len());
        for _i in 0..types_to_sink.len() {
            // wait for messages from all table writing actors
            match rx_delta_table_state.recv().await {
                Some(ActorMessageTypes::DeltaTableState(msg_type, state)) => {
                    currently_sinked_offsets.insert(msg_type, state);
                }
                _ => {
                    warn!("Received an unexpected message from the Table Actor.");
                }
            }
        }
        // find the min for each partition
        let offsets_to_seek =
            currently_sinked_offsets
                .into_values()
                .flatten()
                .fold(HashMap::new(), |mut acc, e| {
                    acc.entry(e.0)
                        .and_modify(|v| *v = min(*v, e.1))
                        .or_insert(e.1);
                    acc
                });
        offsets_to_seek
    }

    fn setup_deserialization_actors(
        &self,
        rx_raw_message: async_channel::Receiver<ActorMessageTypes>,
        queue_size: usize,
    ) -> Receiver<ActorMessageTypes> {
        let (tx_deserialized, rx_deserialized) = mpsc::channel(queue_size);
        for _ in 0..self.opts.deserialization_workers.clone() {
            let r = rx_raw_message.clone();
            let t = tx_deserialized.clone();
            let deserializer = self.message_deserializer;
            //let deserializer = self.message_deserializer.clone();
            spawn_blocking(move || {
                let mda = MessageDeserializingActor::new(r, t, deserializer);
                block_on(async { mda.start().await });
            });
        }
        rx_deserialized
    }

    async fn setup_pre_existing_tables(
        &self,
        transformer: &Transformer,
        types_to_sink: &mut HashMap<MessageType, Sender<ActorMessageTypes>>,
        dlq_tx: &mut Sender<ActorMessageTypes>,
        tx_delta_table_state: &Sender<ActorMessageTypes>,
        checkpoint_mutex: Arc<Mutex<()>>,
    ) -> Result<(), IngestError> {
        let entries =
            read_entries_from_s3(&self.opts.table_base_uri, &self.message_type_whitelist).await;
        match entries {
            Ok(entries) => {
                // init for existing delta tables
                for existing_table_name in entries {
                    let dtsa_tx = self
                        .setup_new_message_type_table_sink_actor(
                            dlq_tx.clone(),
                            tx_delta_table_state.clone(),
                            &existing_table_name,
                            transformer.clone(),
                            build_metrics(&self.opts)?,
                            checkpoint_mutex.clone(),
                        )
                        .await;
                    match dtsa_tx {
                        Ok(tx) => {
                            types_to_sink.insert(MessageType::from(&existing_table_name), tx);
                        }
                        Err(e) => {
                            error!("Failed to setup new message type actor sequence for existing table: [{}] due to: [{}]", existing_table_name, e);
                        }
                    }
                }
                Ok(())
            }
            Err(e) => {
                warn!("Failed to read entries from S3 due to: [{}]", e);
                Ok(())
            }
        }
    }

    /// Seeks the Kafka consumer to the appropriate offsets based on the [`PartitionAssignment`].
    fn seek_consumer(&self, partition_assignment: &PartitionAssignment) -> Result<(), IngestError> {
        let mut log_message = String::new();

        for (p, offset) in partition_assignment.assignment.iter() {
            match offset {
                Some(o) if *o == 0 => {
                    // MARK: workaround for rdkafka error when attempting seek to offset 0
                    info!("Seeking consumer to beginning for partition [{}]. Delta log offset is 0, but seek to zero is not possible.", p);
                    self.consumer
                        .seek(&self.topic, *p, Offset::Beginning, Timeout::Never)?;
                }
                Some(o) => {
                    info!("Seeking consumer to Offset [{}] for partition [{}].", o, p);
                    self.consumer
                        .seek(&self.topic, *p, Offset::Offset(*o), Timeout::Never)?;

                    log_message = log_message.add(format!("{}:{},", p, o).as_str());
                }
                None => match self.opts.auto_offset_reset {
                    AutoOffsetReset::Earliest => {
                        info!("Seeking consumer to beginning for partition [{}]. Partition has no stored offset but 'auto.offset.reset' is earliest", p);
                        self.consumer
                            .seek(&self.topic, *p, Offset::Beginning, Timeout::Never)?;
                    }
                    AutoOffsetReset::Latest => {
                        info!("Seeking consumer to end for partition [{}]. Partition has no stored offset but 'auto.offset.reset' is latest", p);
                        self.consumer
                            .seek(&self.topic, *p, Offset::End, Timeout::Never)?;
                    }
                },
            };
        }
        if !log_message.is_empty() {
            info!("Seeking consumer to partition offsets: [{}]", log_message);
        }
        Ok(())
    }

    async fn setup_new_message_type_table_sink_actor(
        &self,
        dlq_tx: Sender<ActorMessageTypes>,
        delta_table_state_tx: Sender<ActorMessageTypes>,
        table_name: &String,
        transformer: Transformer,
        metrics: Box<dyn IngestMetrics + Send + Sync>,
        checkpoint_mutex: Arc<Mutex<()>>,
    ) -> Result<Sender<ActorMessageTypes>, IngestError> {
        let table_url = String::from(self.table_base_uri.as_str())
            + "/"
            + &table_name.clone().to_string().as_str();
        debug!("Loading Table from [{}]", table_url);
        let num_conversion_actors = self.opts.num_converting_workers;
        let table = load_table(&table_url, HashMap::new()).await?;
        // set the latency to a random value within 10% of the allowed latency. this prevents transaction conflicts.
        let rand_latency_component: u64 =
            (self.opts.allowed_latency as f64 * 0.1 * rand::random::<f64>()) as u64;
        let allowed_latency_with_rand =
            Duration::from_secs(self.opts.allowed_latency + rand_latency_component);

        let (dtsa, dtsa_tx) = DeltaTableSinkActor::new(
            table,
            table_name.to_string(),
            allowed_latency_with_rand,
            self.opts.max_messages_per_batch,
            dlq_tx.clone(),
            self.opts.min_bytes_per_file,
            self.opts.write_checkpoints,
            self.opts.app_id.clone(),
            self.opts.deserialization_workers * num_conversion_actors, // this seems a reasonable heuristic. might need tweaking in the future.
            metrics,
            delta_table_state_tx,
            checkpoint_mutex,
            num_conversion_actors,
            self.opts.num_delta_write_threads,
            transformer,
            self.opts.max_version_lookback,
        );
        spawn_blocking(move || block_on(async { dtsa.start().await }));
        Ok(dtsa_tx)
    }
    async fn get_messages_from_receiver(
        &'static self,
        types_to_sink: &HashMap<MessageType, Sender<ActorMessageTypes>>,
    ) -> Result<Vec<BorrowedMessage>, IngestError> {
        let num_messages_to_retrieve =
            self.opts.deserialization_workers * self.worker_to_messages_multiplier;
        let mut messages = Vec::with_capacity(num_messages_to_retrieve);
        let idle_seconds =
            Duration::from_secs(self.opts.seconds_idle_kafka_read_before_flush as u64);

        let mut fail_count = 0;
        for _i in 0..num_messages_to_retrieve {
            let msg_or_timeout = tokio::time::timeout(idle_seconds, self.consumer.recv()).await;
            match msg_or_timeout {
                Ok(Ok(message)) => {
                    messages.push(message);
                }
                Ok(Err(e)) => {
                    error!("Error receiving message: {:?}", e);

                    match e {
                        KafkaError::Global(_) | KafkaError::MessageConsumption(_) => {
                            fail_count += 1;
                            if fail_count > MAX_KAFKA_RECEIVE_FAIL_COUNT {
                                error!("Too many kafka failures when trying to receive messages. Aborting.");
                                let types_to_sink = types_to_sink;
                                for c in types_to_sink.values() {
                                    c.send(ActorMessageTypes::Terminate).await.expect("Could not send shutdown message to the handling actors. We must now shut down.");
                                }
                                return Err(IngestError::from(e));
                            }
                            error!("Kafka Error while receiving. Trying to recover.");
                            continue;
                        }
                        _ => {
                            error!("Non-Global Kafka Error. Aborting.");
                            let types_to_sink = types_to_sink;
                            for c in types_to_sink.values() {
                                c.send(ActorMessageTypes::Terminate).await.expect("Could not send shutdown message to the handling actors. We must now shut down.");
                            }
                            return Err(IngestError::from(e));
                        }
                    }
                }
                Err(timeout_error) => {
                    warn!(
                        "Timeout after {:?} while waiting to receive a message.",
                        timeout_error
                    );
                    break;
                }
            }
        }
        Ok(messages)
    }
    async fn create_new_delta_table(
        &self,
        message_type: &MessageType,
        template_value: &Value,
    ) -> Result<(), IngestError> {
        let table_name = message_type.to_string();
        let table_url = String::from(self.table_base_uri.as_str())
            + "/"
            + &table_name.clone().to_string().as_str();
        // pretty json print the template value
        debug!(
            "Creating new table at [{}] with template value: {}",
            table_url,
            serde_json::to_string_pretty(template_value).unwrap()
        );

        let columns = create_columns(template_value);
        debug!(
            "Creating new table at [{}] with columns: {:?}",
            table_url, columns
        );

        // derive partition columns. should be all columns which exist both in columns and self.default_partition_columns
        let column_set: HashSet<_> = columns
            .iter()
            .map(|e| String::from(e.name.clone()))
            .collect();
        info!(
            "Default Partition Column Set: {:?}",
            self.opts.default_partition_columns
        );
        let partition_columns: Vec<_> = self
            .opts
            .default_partition_columns
            .iter()
            .filter(|col| column_set.contains(*col))
            .collect();
        info!(
            "Creating new table at [{}] with partition columns: {:?}",
            table_url, partition_columns
        );
        let _table = deltalake_core::operations::create::CreateBuilder::new()
            .with_location(&table_url)
            .with_table_name(table_name)
            .with_columns(columns)
            .with_partition_columns(partition_columns)
            .await?;

        Ok(())
    }

    /// Return true if we did a rebalance which requires us to reset where we consume from on the kafka side.
    async fn handle_rebalance(
        &self,
        rebalance_signal: Arc<RwLock<Option<RebalanceSignal>>>,
        types_to_sink: &HashMap<MessageType, Sender<ActorMessageTypes>>,
        current_assignment: &mut PartitionAssignment,
    ) -> Result<bool, IngestError> {
        // step 1 - use a read lock so we don't starve the write lock from `KafkaContext` to check if a rebalance signal exists.
        // if there is a rebalance assign signal - in step 2, we grab a write lock so we can reset state and clear signal.
        let rebalance_action = {
            let rebalance_signal = rebalance_signal.read().await;
            if let Some(rb) = rebalance_signal.as_ref() {
                match rb {
                    RebalanceSignal::RebalanceAssign(_) => {
                        Some(RebalanceAction::ClearStateAndSkipMessage)
                    }
                    _ => Some(RebalanceAction::SkipMessage),
                }
            } else {
                None
            }
        };

        // step 2 - if there is a rebalance assign signal - we need to acquire the write lock so we can clear it after resetting state.
        // if there is a revoke signal - we should skip the message, but not bother altering state yet.
        match rebalance_action {
            Some(RebalanceAction::ClearStateAndSkipMessage) => {
                let rebalance_signal_val = rebalance_signal.write().await.take();
                match rebalance_signal_val {
                    Some(RebalanceSignal::RebalanceAssign(partitions)) => {
                        info!(
                            "Handling rebalance assign. Assigned partitions are {:?}",
                            partitions
                        );
                        // check if any partitions changed
                        let (adds, removes) = current_assignment.new_assignment(&partitions);
                        if adds.len() == 0 && removes.len() == 0 {
                            return Ok(false);
                        }
                        for c in types_to_sink.values() {
                            match c
                                .send(ActorMessageTypes::Rebalance(adds.clone(), removes.clone()))
                                .await
                            {
                                Ok(_) => {}
                                Err(e) => {
                                    error!("Failed to send rebalance signal to actor: {:?}", e);
                                    let e = IngestError::SendError {};
                                    return Err(e);
                                }
                            }
                        }

                        Ok(true)
                    }
                    _ => unreachable!(),
                }
            }
            Some(RebalanceAction::SkipMessage) => {
                error!("Skipping message while awaiting rebalance");
                Err(IngestError::RebalanceInterrupt)
            }
            None => Ok(false),
        }
    }
}

struct MessageDeserializingActor {
    source: async_channel::Receiver<ActorMessageTypes>,
    sink: Sender<ActorMessageTypes>,
    deserializer: &'static (dyn MessageDeserializer + Send + Sync),
}

impl MessageDeserializingActor {
    pub fn new(
        source: async_channel::Receiver<ActorMessageTypes>,
        sink: Sender<ActorMessageTypes>,
        deserializer: &'static (dyn MessageDeserializer + Send + Sync),
    ) -> Self {
        Self {
            source,
            sink,
            deserializer,
        }
    }
    pub async fn start(self) -> () {
        loop {
            trace!("Message Deserializing Actor Waiting for message");
            match self.source.recv().await {
                Err(async_channel::RecvError) | Ok(ActorMessageTypes::Terminate) => {
                    // Upstream has terminated. Do a graceful wrap up
                    debug!("Message Deserializing Actor Received termination signal");
                    return ();
                }
                Ok(ActorMessageTypes::MessageToDeserialize(msg)) => {
                    match msg.payload() {
                        None => {
                            self.sink
                                .send(ActorMessageTypes::DeserializeFailedMessage(msg, None))
                                .await
                                .expect("Failed to Send to Sink. Aborting.");
                        }
                        Some(bytes) => {
                            let deserialized = self.deserializer.deserialize(bytes).await;
                            match deserialized {
                                Ok((message_type, value)) => {
                                    self.sink
                                        .send(ActorMessageTypes::NewDeserializedMessage(
                                            msg,
                                            message_type,
                                            value,
                                        ))
                                        .await
                                        .expect("Failed to Send to Sink. Aborting.");
                                }
                                Err(e) => {
                                    // send to DLQ
                                    self.sink
                                        .send(ActorMessageTypes::DeserializeFailedMessage(
                                            msg,
                                            Some(e),
                                        ))
                                        .await
                                        .expect("Failed to Send to Sink. Aborting.");
                                }
                            }
                        }
                    }
                }
                _ => {
                    warn!("Message Deserializing Actor Received unexpected message.");
                }
            }
        }
    }
}

struct MessageConvertingActor {
    message_type: MessageType,
    source: async_channel::Receiver<ActorMessageTypes>,
    sink: Sender<ActorMessageTypes>,
    dlq: Sender<ActorMessageTypes>,
    transformer: Transformer,
    coercion_tree: CoercionTree,
}

impl MessageConvertingActor {
    pub fn new(
        sink: Sender<ActorMessageTypes>,
        dlq: Sender<ActorMessageTypes>,
        transformer: Transformer,
        coercion_tree: CoercionTree,
        message_type: MessageType,
        source: async_channel::Receiver<ActorMessageTypes>,
    ) -> Self {
        Self {
            message_type,
            source,
            sink,
            dlq,
            transformer,
            coercion_tree,
        }
    }
    pub async fn start(self) -> () {
        let mut loop_count = 0u64;
        let mut start_time = Instant::now();
        loop {
            trace!(
                "Message Converting Actor [{}] Waiting for message",
                self.message_type
            );
            match self.source.recv().await {
                Err(_) | Ok(ActorMessageTypes::Terminate) => {
                    // Upstream has terminated. Do a graceful wrap up
                    debug!(
                        "Message Converting Actor [{}] Received termination signal",
                        self.message_type
                    );
                    return ();
                }
                Ok(ActorMessageTypes::NewDeserializedMessage(
                    src_msg,
                    _message_type,
                    mut value,
                )) => {
                    let t = self.transformer.transform(&mut value, Some(&src_msg));
                    match t {
                        Ok(_) => {
                            // remove any MAP_MARKER_ENTRY entries since we do not want these in the final table.
                            #[cfg(feature = "akunaformats")]
                            remove_map_marker_entries(&mut value);
                            coercions::coerce(&mut value, &self.coercion_tree);
                            trace!("{}", value.to_string());
                            self.sink
                                .send(ActorMessageTypes::NewDeltaRecord(
                                    DataTypePartition::from(src_msg.partition()),
                                    DataTypeOffset::from(src_msg.offset()),
                                    value,
                                ))
                                .await
                                .expect("Failed to Send to Sink. Aborting.");
                        }
                        Err(e) => {
                            self.sink
                                .send(ActorMessageTypes::FailedTransformRecord(
                                    DataTypePartition::from(src_msg.partition()),
                                    DataTypeOffset::from(src_msg.offset()),
                                    Null,
                                ))
                                .await
                                .expect("Failed to Send to Sink. Aborting.");

                            // send to DLQ
                            let dead_letter = DeadLetter::from_failed_transform(&value, e);
                            self.dlq
                                .send(ActorMessageTypes::DLQ(dead_letter))
                                .await
                                .expect("Failed to Send to DLQ. Aborting.");
                        }
                    }
                }
                _ => {
                    warn!(
                        "Message Converting Actor [{}] Received unexpected message.",
                        self.message_type
                    );
                }
            }
            loop_count = loop_count + 1;
            if loop_count % 100000 == 0 {
                let elapsed = start_time.elapsed();
                let per_iter = (elapsed.as_micros() as u64) / loop_count;
                trace!(
                    "Message Converting Actor [{}] Completed [{}] iterations in [{:?}] which is [{:?}] micros per iteration",
                    self.message_type, loop_count, elapsed, per_iter
                );
                loop_count = 0;
                start_time = Instant::now();
            }
        }
    }
}

struct RecordBatchCompletion {
    data_writer: DataWriter,
    record_count: usize,
    start_time: Instant,
}

struct DeltaTableSinkActor {
    input: Receiver<ActorMessageTypes>,
    table: DeltaTable,
    value_buffers: ValueBuffers,
    table_name: String,
    table_write_timeout: Duration,
    table_last_write: Instant,
    max_messages_per_batch: usize,
    delta_writer: Option<Box<Arc<Mutex<DataWriter>>>>,
    dlq: Sender<ActorMessageTypes>,
    delta_table_state_tx: Sender<ActorMessageTypes>,
    min_bytes_per_file: usize,
    write_checkpoints: bool,
    app_id: String,
    metrics: Box<dyn IngestMetrics + Send + Sync>,
    partition_assignment: HashMap<DataTypePartition, Option<DataTypeOffset>>,
    checkpoint_mutex: Arc<Mutex<()>>,
    num_conversion_actors: usize,
    transformer: Transformer,
    curr_buffer_len: usize,
    max_version_lookback: usize,
}

impl DeltaTableSinkActor {
    pub fn new(
        table: DeltaTable,
        table_name: String,
        table_write_timeout: Duration,
        max_messages_per_batch: usize,
        dlq: Sender<ActorMessageTypes>,
        min_bytes_per_file: usize,
        write_checkpoints: bool,
        app_id: String,
        message_queue_size: usize,
        metrics: Box<dyn IngestMetrics + Send + Sync>,
        delta_table_state_tx: Sender<ActorMessageTypes>,
        checkpoint_mutex: Arc<Mutex<()>>,
        num_conversion_actors: usize,
        num_delta_write_threads: usize,
        transformer: Transformer,
        max_version_lookback: usize,
    ) -> (Self, Sender<ActorMessageTypes>) {
        let (tx, rx) = mpsc::channel(message_queue_size);
        let mut writer_options = HashMap::new();
        writer_options.insert(
            "num_threads".to_string(),
            format!("{}", num_delta_write_threads),
        );
        let delta_writer = Some(Box::new(Arc::new(Mutex::new(
            DataWriter::for_table(&table, writer_options).expect("Failed to create DataWriter"),
        ))));

        (
            Self {
                input: rx,
                table,
                value_buffers: ValueBuffers::default(),
                table_name: table_name,
                table_write_timeout,
                table_last_write: Instant::now(),
                max_messages_per_batch,
                delta_writer,
                dlq,
                delta_table_state_tx,
                min_bytes_per_file,
                write_checkpoints,
                app_id,
                metrics,
                partition_assignment: HashMap::new(),
                checkpoint_mutex: checkpoint_mutex,
                num_conversion_actors,
                transformer,
                curr_buffer_len: 0,
                max_version_lookback,
            },
            tx,
        )
    }
    pub async fn start(mut self) -> () {
        // create N conversion actors
        let (mca_tx, mca_rx) =
            async_channel::bounded::<ActorMessageTypes>(self.num_conversion_actors * 10);
        let (converted_msg_tx, mut converted_msg_rx) =
            mpsc::channel::<ActorMessageTypes>(self.num_conversion_actors * 10);

        self.init_message_converting_actors(mca_rx, converted_msg_tx);
        let max_concurrent_record_batch_tasks = 5;
        let mut queued_record_batch_tasks = Vec::new();
        let mut completed_record_batches = Vec::new();
        loop {
            trace!("Delta Sink Actor [{}] Waiting for message", self.table_name);
            // receive message from queue
            let v = self.input.recv().await;
            match v {
                None | Some(ActorMessageTypes::Terminate) => {
                    let _still_pendingtasks = match self
                        .collect_completed_record_batches(
                            queued_record_batch_tasks,
                            &mut completed_record_batches,
                        )
                        .await
                    {
                        Some(value) => value,
                        None => return,
                    };
                    self.finalize_inflight_data(completed_record_batches).await;
                    return;
                }
                Some(ActorMessageTypes::DeltaRecordBatchSet(set_size, msg_rx)) => {
                    if !self
                        .handle_new_recordbatch(
                            mca_tx.clone(),
                            &mut converted_msg_rx,
                            set_size,
                            msg_rx,
                        )
                        .await
                    {
                        return;
                    }
                }
                Some(ActorMessageTypes::Flush) => {
                    debug!(
                        "Delta Sink Actor [{}] Received flush signal",
                        self.table_name
                    );
                }
                Some(ActorMessageTypes::Rebalance(adds, removes)) => {
                    self.handle_rebalance(adds, removes).await;
                    // also empty out currently running tasks
                    queued_record_batch_tasks.clear();
                }
                _ => {
                    // received an upexpected message. Log and ignore
                    debug!(
                        "Delta Table Sink Actor [{}] Received unexpected message.",
                        self.table_name
                    );
                }
            }
            // check if we need to flush buffer
            if self.should_complete_record_batch() {
                self.metrics.batch_started(self.table_name.as_str());
                // send the batch completion to a new thread
                match self.spawn_complete_record_batch().await {
                    Err(e) => {
                        // log error
                        error!(
                            "Delta Table Sink Actor [{}] Failed to complete record batch: {:?}",
                            self.table_name, e
                        );
                        // panic!("Failed to complete record batch");
                    }
                    Ok(join_handle) => {
                        queued_record_batch_tasks.push(join_handle);
                    }
                }
            }
            // check if any prior record batch tasks have completed
            loop {
                let still_pendingtasks = match self
                    .collect_completed_record_batches(
                        queued_record_batch_tasks,
                        &mut completed_record_batches,
                    )
                    .await
                {
                    Some(value) => value,
                    None => return,
                };
                // swap the still pending tasks back into the queue
                queued_record_batch_tasks = still_pendingtasks;
                if queued_record_batch_tasks.len() < max_concurrent_record_batch_tasks {
                    break;
                }
            }

            // check if we need to write files
            if self.should_complete_file() {
                let start_of_write = Instant::now();
                match self
                    .complete_file(std::mem::take(&mut completed_record_batches))
                    .await
                {
                    Err(e) => {
                        // log error
                        error!(
                            "Delta Table Sink Actor [{}] Failed to write file: {:?}",
                            self.table_name, e
                        );
                        self.metrics.delta_write_failed(self.table_name.as_str());
                        panic!("Failed to write file.");
                    }
                    _ => {
                        self.metrics
                            .delta_write_completed(&start_of_write, self.table_name.as_str());
                    }
                }
                self.table_last_write = Instant::now();
            }
            // check for schema change.
        }
    }

    async fn collect_completed_record_batches(
        &mut self,
        queued_record_batch_tasks: Vec<JoinHandle<Result<RecordBatchCompletion, IngestError>>>,
        completed_record_batches: &mut Vec<DataWriter>,
    ) -> Option<Vec<JoinHandle<Result<RecordBatchCompletion, IngestError>>>> {
        let mut still_pendingtasks = Vec::new();
        // we have to strictly write batches in sequence so we dont risk gaps on restart.
        let mut pending_task_hit = false;
        for jh in queued_record_batch_tasks.into_iter() {
            if jh.is_finished() == false || pending_task_hit {
                still_pendingtasks.push(jh);
                pending_task_hit = true;
                continue;
            }
            let r = jh.await.expect("Failed to join record batch task");
            match r {
                Ok(recordbatch_completion) => {
                    let records = recordbatch_completion.record_count;
                    self.curr_buffer_len += recordbatch_completion.data_writer.buffer_len();
                    self.metrics.batch_completed(
                        records,
                        &recordbatch_completion.start_time,
                        self.table_name.as_str(),
                    );
                    // put the batch somewhere to save later into files
                    completed_record_batches.push(recordbatch_completion.data_writer);
                }
                Err(e) => {
                    error!(
                        "Delta Table Sink Actor [{}] Failed to complete record batch while collecting completed record batch: {:?}",
                        self.table_name, e
                    );
                    //return None;
                }
            }
        }
        Some(still_pendingtasks)
    }

    fn init_message_converting_actors(
        &mut self,
        mca_rx: async_channel::Receiver<ActorMessageTypes>,
        converted_msg_tx: Sender<ActorMessageTypes>,
    ) {
        debug!(
            "Creating [{}] Message Converting Actors",
            self.num_conversion_actors
        );
        let mut mcas = Vec::with_capacity(self.num_conversion_actors);
        for _i in 0..self.num_conversion_actors {
            let coercion_tree = self
                .get_coercion_tree()
                .expect("Failed to get coercion tree");
            let mca = MessageConvertingActor::new(
                converted_msg_tx.clone(),
                self.dlq.clone(),
                self.transformer.clone(),
                coercion_tree,
                MessageType::from(&self.table_name.clone()),
                mca_rx.clone(),
            );
            mcas.push(mca);
        }
        for mca in mcas.into_iter() {
            spawn_blocking(move || block_on(async { mca.start().await }));
        }
    }

    async fn handle_rebalance(
        &mut self,
        adds: HashSet<DataTypePartition>,
        removes: HashSet<DataTypePartition>,
    ) {
        info!(
            "Delta Table Sink Actor [{}] Received re-balance request.",
            self.table_name
        );
        // always reset the buffers since the kafka reader will be reset to the latest written offset on re-balance.
        self.value_buffers.reset();
        // reset the timers so we don't write too often
        self.table_last_write = Instant::now();

        for e in adds {
            self.partition_assignment.insert(e, None);
        }
        for e in removes {
            self.partition_assignment.remove(&e);
        }
        let mut latest_safe_offsets = HashMap::new();
        for partition in self.partition_assignment.keys() {
            let txn_app_id = txn_app_id_for_partition(self.app_id.as_str(), *partition);
            // we need to step through a number of prior table versions to ensure we get all partition offsets.
            let version = last_txn_version(
                &mut self.table,
                &txn_app_id,
                self.table_name.as_str(),
                self.max_version_lookback,
            )
            .await;
            if let Some(version) = version {
                latest_safe_offsets.insert(*partition, version);
            }
        }
        // update self.partition_assignment with the offsets we found
        for (partition, offset) in latest_safe_offsets.iter() {
            self.partition_assignment.insert(*partition, Some(*offset));
        }
        self.delta_table_state_tx
            .send(ActorMessageTypes::DeltaTableState(
                self.table_name.clone(),
                latest_safe_offsets,
            ))
            .await
            .expect("Failed to send Offsets back");
    }

    async fn handle_new_recordbatch(
        &mut self,
        mca_tx: async_channel::Sender<ActorMessageTypes>,
        mut converted_msg_rx: &mut Receiver<ActorMessageTypes>,
        set_size: usize,
        msg_rx: Receiver<ActorMessageTypes>,
    ) -> bool {
        trace!(
            "Delta Sink Actor [{}] Received DeltaRecordBatchSet signal for size [{}]",
            self.table_name,
            set_size
        );

        let start_time_convert = Instant::now();
        match self
            .convert_delta_record_batch_set(mca_tx.clone(), &mut converted_msg_rx, set_size, msg_rx)
            .await
        {
            Ok((msg_count, failed_transform, mut delta_records)) => {
                trace!(
                    "Delta Sink Actor [{}] Converted [{}] messages and failed to convert [{}]",
                    self.table_name,
                    msg_count,
                    failed_transform
                );
                self.metrics
                    .message_transform_failed(self.table_name.as_str(), failed_transform as i64);
                self.metrics
                    .transform_completed(&start_time_convert, self.table_name.as_str());
                match self.process_delta_records(&mut delta_records).await {
                    Ok(_) => {}
                    Err(err) => {
                        error!(
                            "Delta Sink Actor [{}] Failed to process delta records: {:?}",
                            self.table_name, err
                        );
                        return false;
                    }
                }
            }
            Err(err) => {
                error!(
                    "Delta Sink Actor [{}] Failed to handle delta record batch set: {:?}",
                    self.table_name, err
                );
                return false;
            }
        }
        true
    }

    async fn finalize_inflight_data(mut self, completed_record_batches: Vec<DataWriter>) {
        info!(
            "Delta Sink Actor [{}] Received termination signal",
            self.table_name
        );
        self.complete_record_batch()
            .await
            .expect("Failed to flush buffer while trying to terminate: Aborting.");
        self.complete_file(completed_record_batches)
            .await
            .expect("Failed to write file while trying to terminate: Aborting.");
    }

    async fn convert_delta_record_batch_set(
        &self,
        mca_tx: async_channel::Sender<ActorMessageTypes>,
        converted_msg_rx: &mut Receiver<ActorMessageTypes>,
        set_size: usize,
        msg_rx: Receiver<ActorMessageTypes>,
    ) -> Result<
        (
            usize, // converted_message_count
            usize, // failed_convert_count
            Vec<(DataTypePartition, DataTypeOffset, Value)>,
        ),
        String,
    > {
        let mut failed_transforms = 0;
        let mut converted_messages = 0;

        // loop for set_size and dispatch to the conversion actors
        trace!(
            "Delta Sink Actor [{}] Sending  [{}] DeltaRecords for conversion",
            self.table_name,
            set_size
        );
        let my_mca_send_tx = mca_tx.clone();
        let _handle = spawn(forward_messages_to_sink(msg_rx, my_mca_send_tx));

        // now loop to receive all the converted messages
        let mut loop_count = 0;
        trace!(
            "Delta Sink Actor [{}] Receiving [{}] DeltaRecords from conversion",
            self.table_name,
            set_size
        );

        let mut delta_records: Vec<(DataTypePartition, DataTypeOffset, Value)> =
            Vec::with_capacity(self.max_messages_per_batch);
        loop {
            if loop_count == set_size {
                break;
            }
            let msg = converted_msg_rx.recv().await;
            loop_count += 1;
            match msg {
                None => {
                    // the channel is closed. We should break out of the loop
                    debug!(
                        "Delta Sink Actor [{}] Conversion channel closed after [{}] messages",
                        self.table_name, loop_count
                    );
                    break;
                }
                Some(converted_msg) => {
                    match converted_msg {
                        ActorMessageTypes::NewDeltaRecord(partition, offset, value) => {
                            converted_messages += 1;
                            delta_records.push((partition, offset, value));
                        }
                        ActorMessageTypes::FailedTransformRecord(partition, offset, _value) => {
                            debug!(
                                            "Delta Sink Actor [{}] Received failed transform message for partition: [{}] and offset: [{}]",
                                            self.table_name,
                                            partition,
                                            offset
                                        );
                            failed_transforms += 1;
                        }
                        _ => {
                            // received an upexpected message. Log and ignore
                            debug!(
                                "Delta Sink Actor [{}] Received unexpected message.",
                                self.table_name
                            );
                        }
                    }
                }
            }
        }

        Ok((converted_messages, failed_transforms, delta_records))
    }

    fn should_complete_record_batch(&self) -> bool {
        let elapsed_millis = self.table_last_write.elapsed().as_millis();
        let should = self.value_buffers.len() > 0
            && (self.value_buffers.len() >= self.max_messages_per_batch
                || (elapsed_millis >= (self.table_write_timeout.as_secs() * 1000) as u128));

        trace!(
            "Should complete record batch - latency test: {} >= {}",
            elapsed_millis,
            (self.table_write_timeout.as_secs() * 1000) as u128
        );
        trace!(
            "Should complete record batch - buffer length test: {} >= {}",
            self.value_buffers.len(),
            self.max_messages_per_batch
        );
        should
    }

    async fn spawn_complete_record_batch(
        &mut self,
    ) -> Result<JoinHandle<Result<RecordBatchCompletion, IngestError>>, IngestError> {
        debug!(
            "Spawning to Complete record batch for table [{}]",
            self.table_name.as_str()
        );
        let ConsumedBuffers {
            values,
            partition_offsets,
            partition_counts,
        } = self.value_buffers.consume();

        // set the partition offsets so we can include them in the commit
        for (partition, offset) in partition_offsets.iter() {
            self.partition_assignment.insert(*partition, Some(*offset));
        }

        let delta_writer = self.delta_writer.as_mut().unwrap().lock().await;
        let mut my_delta_writer = delta_writer.duplicate();
        drop(delta_writer);
        let my_dlq = self.dlq.clone();
        let h = spawn_blocking(move || {
            block_on(async {
                let record_count = values.len();
                let start_time = Instant::now();
                if let Err(e) = my_delta_writer.write(values).await {
                    if let DataWriterError::PartialParquetWrite {
                        skipped_values,
                        sample_error,
                    } = *e
                    {
                        warn!(
                            "Partial parquet write, skipped {} values, sample ParquetError {:?}",
                            skipped_values.len(),
                            sample_error
                        );
                        let dead_letters = DeadLetter::vec_from_failed_parquet_rows(skipped_values);
                        for dead_letter in dead_letters {
                            my_dlq
                                .send(ActorMessageTypes::DLQ(dead_letter))
                                .await
                                .expect("Failed to send to DLQ. Aborting.");
                        }
                    } else {
                        // print details about the failed batch
                        error!("Unhandled error on write for table");
                        // error!("Tried to writed these values: {:?}", values);

                        return Err(IngestError::DeltaWriteFailed {
                            ending_offsets: serde_json::to_string(&partition_offsets).unwrap(),
                            partition_counts: serde_json::to_string(&partition_counts).unwrap(),
                            source: *e,
                        });
                    }
                }
                let recordbatch_completion = RecordBatchCompletion {
                    data_writer: my_delta_writer,
                    record_count,
                    start_time,
                };
                Ok(recordbatch_completion)
            })
        });

        Ok(h)
    }

    async fn complete_record_batch(&mut self) -> Result<(), IngestError> {
        debug!(
            "Completing record batch for table [{}]",
            self.table_name.as_str()
        );
        let ConsumedBuffers {
            values,
            partition_offsets,
            partition_counts,
        } = self.value_buffers.consume();

        if values.is_empty() {
            return Ok(());
        }
        // set the partition offsets so we can include them in the commit
        for (partition, offset) in partition_offsets.iter() {
            self.partition_assignment.insert(*partition, Some(*offset));
        }

        let mut delta_writer = self.delta_writer.as_mut().unwrap().lock().await;

        if let Err(e) = delta_writer.write(values).await {
            if let DataWriterError::PartialParquetWrite {
                skipped_values,
                sample_error,
            } = *e
            {
                warn!(
                    "Partial parquet write, skipped {} values, sample ParquetError {:?}",
                    skipped_values.len(),
                    sample_error
                );

                let dead_letters = DeadLetter::vec_from_failed_parquet_rows(skipped_values);
                for dead_letter in dead_letters {
                    self.dlq
                        .send(ActorMessageTypes::DLQ(dead_letter))
                        .await
                        .expect("Failed to send to DLQ. Aborting.");
                }
            } else {
                return Err(IngestError::DeltaWriteFailed {
                    ending_offsets: serde_json::to_string(&partition_offsets).unwrap(),
                    partition_counts: serde_json::to_string(&partition_counts).unwrap(),
                    source: *e,
                });
            }
        }
        self.curr_buffer_len = delta_writer.buffer_len();

        Ok(())
    }

    fn should_complete_file(&self) -> bool {
        let elapsed_secs = self.table_last_write.elapsed().as_secs();

        let should = self.curr_buffer_len > 0
            && (self.curr_buffer_len >= self.min_bytes_per_file
                || elapsed_secs >= self.table_write_timeout.as_secs());

        trace!(
            "Should complete file - latency test: {} >= {:?}",
            elapsed_secs,
            self.table_write_timeout
        );
        if self.delta_writer.is_some() {
            trace!(
                "Should complete file - num bytes test: {} >= {}",
                self.curr_buffer_len,
                self.min_bytes_per_file
            );
        }

        should
    }
    /// Writes parquet buffers to a file in the destination delta table.
    async fn complete_file(
        &mut self,
        completed_record_batches: Vec<DataWriter>,
    ) -> Result<i64, IngestError> {
        self.metrics.delta_write_started(self.table_name.as_str());
        self.metrics.record_stat(
            StatType::DeltaWriteNumPartitions,
            self.partition_assignment.len() as i64,
            Some(self.table_name.as_str()),
        );
        self.metrics.record_stat(
            StatType::DeltaWriteSize,
            self.value_buffers.len() as i64,
            Some(self.table_name.as_str()),
        );

        // TODO: are these right?
        let partition_offsets = self.partition_offsets();
        // Upload pending parquet file to delta store
        // TODO: remove it if we got conflict error? or it'll be considered as tombstone
        info!(
            "Writing parquet files to delta store for table [{}]",
            self.table_name.as_str()
        );
        // let mut delta_writer = self.delta_writer.as_mut().unwrap().lock().await;
        // let add = delta_writer
        //     .write_parquet_files(&self.table.table_uri())
        //     .await?;

        let mut add = Vec::new();
        for mut record_batch in completed_record_batches {
            let mut r = record_batch
                .write_parquet_files(&self.table.table_uri())
                .await?;
            add.append(&mut r);
        }

        // TODO: Handle Offset Conflicts
        // if !self.are_partition_offsets_match() {
        //     return Err(IngestError::ConflictingOffsets);
        // }

        // TODO: Handle Schema Change
        // if self
        //     .delta_writer
        //     .as_mut()
        //     .unwrap()
        //     .update_schema(self.table.as_mut().unwrap().state.delta_metadata().unwrap())?
        // {
        //     info!("Table schema has been updated");
        //     // Update the coercion tree to reflect the new schema
        //     let mut coercion_tree = coercions::create_coercion_tree(self.table.as_mut().unwrap().schema().unwrap());
        //     let _ = std::mem::replace(&mut self.coercion_tree.as_mut().unwrap(), &mut coercion_tree);
        //
        //     return Err(IngestError::DeltaSchemaChanged);
        // }

        // Try to commit
        trace!("building actions");
        let mut attempt_number: u32 = 0;
        let actions = build_actions(&partition_offsets, self.app_id.as_str(), add);
        trace!("Commit to delta table");
        loop {
            self.table.update().await?;
            let epoch_id = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .expect("Time went backwards")
                .as_millis() as i64;
            // log commit
            info!(
                "Committing to delta table with version {} and epoch_id {} for table {}",
                &self.table.state.as_ref().unwrap().version(),
                epoch_id,
                self.table_name.as_str()
            );

            let commit = deltalake_core::operations::transaction::CommitBuilder::default()
                .with_actions(actions.clone())
                .build(
                    self.table.state.as_ref().map(|s| s as &dyn TableReference),
                    self.table.log_store().clone(),
                    DeltaOperation::StreamingUpdate {
                        output_mode: OutputMode::Append,
                        query_id: self.app_id.clone(),
                        epoch_id,
                    },
                )
                .await
                .map_err(DeltaTableError::from);

            match commit {
                Ok(v) => {
                    self.curr_buffer_len = 0;
                    if self.write_checkpoints {
                        try_create_checkpoint(
                            &mut self.table,
                            v.version,
                            Some(self.checkpoint_mutex.clone()),
                        )
                        .await?;
                    }
                    return Ok(v.version);
                }
                Err(e) => match e {
                    DeltaTableError::VersionAlreadyExists(_) => {
                        error!("Transaction attempt failed. Attempts exhausted beyond max_retry_commit_attempts of {} so failing", DEFAULT_DELTA_MAX_RETRY_COMMIT_ATTEMPTS);
                        return Err(e.into());
                    }

                    DeltaTableError::GenericError { source: _ } => {
                        error!("Delta write failed.. DeltaTableError: {}", e);
                        return Err(IngestError::InconsistentState(
                            "The remote dynamodb lock is non-acquirable!".to_string(),
                        ));
                    }
                    _ if attempt_number == 2 => return Err(e.into()),
                    _ => {
                        error!("Attempt to write failed with unexpected error. Going to retry.");
                        attempt_number += 1
                    }
                },
            }
        }
    }

    fn partition_offsets(&self) -> HashMap<DataTypePartition, DataTypeOffset> {
        let mut partition_offsets = HashMap::new();
        for (partition, offset) in self.partition_assignment.iter() {
            match offset {
                Some(o) => {
                    partition_offsets.insert(*partition, *o);
                }
                None => {}
            }
        }
        partition_offsets
    }

    pub(crate) fn get_coercion_tree(&self) -> Result<CoercionTree, IngestError> {
        Ok(coercions::create_coercion_tree(self.table.schema().ok_or(
            IngestError::InconsistentState("Table schema not found".to_string()),
        )?))
    }

    async fn process_delta_records(
        &mut self,
        delta_records: &mut Vec<(DataTypePartition, DataTypeOffset, Value)>,
    ) -> Result<(), IngestError> {
        // Sort the records by partition and offset
        // sort the messages, so we ensure we dont have an out-of-order add, which would trigger a skip due to the offset high watermark tracking.
        trace!("Sorting Messages for table [{}]", self.table_name);
        delta_records.sort_unstable_by(|a, b| {
            if a.0 != b.0 {
                // first sort by partition
                a.0.cmp(&b.0)
            } else {
                a.1.cmp(&b.1) // then sort by offset
            }
        });
        // Apply to buffer
        trace!("Appending to value buffers for table [{}]", self.table_name);
        let mut loop_iter = 0usize;
        let mut skip_count = 0usize;
        for (partition, offset, value) in delta_records.drain(..) {
            loop_iter += 1;
            match self.value_buffers.add(partition, offset, value) {
                Ok(_) => {}
                Err(IngestError::AlreadyProcessedPartitionOffset {
                    partition: _partition,
                    offset: _offset,
                    ..
                }) => {
                    skip_count += 1;
                    self.metrics.message_skipped(self.table_name.as_str());
                }
                Err(e) => {
                    // send to DLQ
                    let dead_letter = DeadLetter::from_failed_ingest(e);
                    self.dlq
                        .send(ActorMessageTypes::DLQ(dead_letter))
                        .await
                        .expect("Failed to send to DLQ. Aborting.");
                }
            }
        }
        if skip_count > 0 {
            trace!(
                "Delta Sink Actor [{}] Skipped [{}] messages due to already processed partition offset",
                self.table_name, skip_count
            );
        }
        trace!(
            "Completed processing delta records batch for table [{}]. Added [{}] records.",
            self.table_name,
            loop_iter
        );
        Ok(())
    }
}

struct DLQSink {
    source: Receiver<ActorMessageTypes>,
    dlq: Box<dyn DeadLetterQueue>,
    dlq_batch_size: usize,
}

impl DLQSink {
    pub async fn new(
        dlq: Box<dyn DeadLetterQueue>,
        buffer_size: usize,
    ) -> (Self, Sender<ActorMessageTypes>) {
        let (tx, rx) = mpsc::channel(buffer_size);

        (
            Self {
                source: rx,
                dlq,
                dlq_batch_size: buffer_size,
            },
            tx,
        )
    }
    pub async fn start(mut self) -> () {
        let mut buffer = Vec::with_capacity(self.dlq_batch_size);
        loop {
            match self.source.recv().await {
                None | Some(ActorMessageTypes::Terminate) => {
                    // Upstream has terminated. Do a graceful wrap up
                    debug!("DLQ Sink Received termination signal");
                    return;
                }
                Some(ActorMessageTypes::DLQ(dead_letter)) => {
                    buffer.push(dead_letter);
                    if buffer.len() >= self.dlq_batch_size {
                        let new_buffer =
                            std::mem::replace(&mut buffer, Vec::with_capacity(self.dlq_batch_size));
                        self.dlq
                            .write_dead_letters(new_buffer)
                            .await
                            .expect("Failed to write to DLQ");
                    }
                }
                Some(ActorMessageTypes::Flush) => {
                    if buffer.len() > 0 {
                        let new_buffer =
                            std::mem::replace(&mut buffer, Vec::with_capacity(self.dlq_batch_size));
                        self.dlq
                            .write_dead_letters(new_buffer)
                            .await
                            .expect("Failed to write to DLQ");
                    }
                }
                _ => {
                    // received an upexpected message. Log and ignore
                    debug!("DLQ Sink Received unexpected message.");
                }
            }
        }
    }
}

async fn forward_messages_to_sink(
    mut source: Receiver<ActorMessageTypes>,
    sink: async_channel::Sender<ActorMessageTypes>,
) -> () {
    loop {
        match source.recv().await {
            None | Some(ActorMessageTypes::Terminate) => {
                // Upstream has terminated. Do a graceful wrap up
                trace!("Received termination signal");
                return;
            }
            Some(msg) => {
                sink.send(msg)
                    .await
                    .expect("Failed to send to Sink. Aborting.");
            }
        }
    }
}
