//! Implementations supporting the kafka-delta-ingest daemon

//! ## Feature flags
//!
//! - `dynamic-linking`: Use the `dynamic-linking` feature of the `rdkafka` crate and link to the system's version of librdkafka instead of letting the `rdkafka` crate builds its own librdkafka.

#![deny(warnings)]
#![deny(missing_docs)]

#[macro_use]
extern crate lazy_static;
#[cfg(test)]
extern crate serde_json;
#[macro_use]
extern crate strum_macros;

use chrono::NaiveDate;
use deltalake_core::kernel::PrimitiveType;
use deltalake_core::kernel::{ArrayType, DataType, StructField, StructType};
use deltalake_core::DeltaTableError;
use indexmap::IndexMap;
use log::{debug, error, info, trace};
use rdkafka::{
    config::ClientConfig,
    consumer::{ConsumerContext, Rebalance},
    error::KafkaError,
    ClientContext, TopicPartitionList,
};
use regex::Regex;
use rusoto_core::Region;
use rusoto_s3::{ListObjectsV2Request, S3Client, S3};
use serde_json::Value;
use std::collections::HashSet;
use std::sync::Arc;
use std::{collections::HashMap, env, path::PathBuf};
use tokio::sync::RwLock;
use tokio_util::sync::CancellationToken;
use url::Url;

use serialization::MessageDeserializerFactory;

use crate::offsets::WriteOffsetsError;
use crate::serialization::MessageType;
use crate::{dead_letters::*, metrics::*, transforms::*, writer::DataWriterError};

mod coercions;
/// Doc
pub mod cursor;
mod dead_letters;
mod delta_helpers;
mod metrics;
mod offsets;
mod parallel_processing;

/// Repairs the delta table
pub mod repair;
mod serialization;
mod transforms;
mod value_buffers;
/// Doc
pub mod writer;

/// Type alias for Kafka partition
pub type DataTypePartition = i32;
/// Type alias for Kafka message offset
pub type DataTypeOffset = i64;

/// The default number of times to retry a delta commit when optimistic concurrency fails.
pub(crate) const DEFAULT_DELTA_MAX_RETRY_COMMIT_ATTEMPTS: u32 = 10_000_000;

/// Errors returned by [`start_ingest`] function.
#[derive(thiserror::Error, Debug)]
pub enum IngestError {
    /// Error from [`rdkafka`]
    #[error("Kafka error: {source}")]
    Kafka {
        /// Wrapped [`KafkaError`]
        #[from]
        source: KafkaError,
    },

    /// Failed to send to tokio worker
    #[error("Failed to send to tokio worker")]
    SendError {},

    /// Error from [`deltalake::DeltaTable`]
    #[error("DeltaTable interaction failed: {source}")]
    DeltaTable {
        /// Wrapped [`deltalake::DeltaTableError`]
        #[from]
        source: DeltaTableError,
    },

    /// Error from [`writer::DataWriter`]
    #[error("Writer error: {source}")]
    Writer {
        /// Wrapped [`DataWriterError`]
        #[from]
        source: Box<DataWriterError>,
    },

    /// Error from [`WriteOffsetsError`]
    #[error("WriteOffsets error: {source}")]
    WriteOffsets {
        /// Wrapped [`WriteOffsetsError`]
        #[from]
        source: WriteOffsetsError,
    },

    /// Error from [`DeadLetterQueue`]
    #[error("DeadLetterQueue error: {source}")]
    DeadLetterQueueError {
        /// Wrapped [`DeadLetterQueueError`]
        #[from]
        source: DeadLetterQueueError,
    },

    /// Error from [`transforms`]
    #[error("TransformError: {source}")]
    Transform {
        /// Wrapped [`TransformError`]
        #[from]
        source: TransformError,
    },

    /// Error from [`serde_json`]
    #[error("JSON serialization failed: {source}")]
    SerdeJson {
        /// Wrapped [`serde_json::Error`]
        #[from]
        source: serde_json::Error,
    },

    /// Error from [`std::io`]
    #[error("IO Error: {source}")]
    IoError {
        /// Wrapped [`std::io::Error`]
        #[from]
        source: std::io::Error,
    },

    /// Error returned when [`IngestMetrics`] fails.
    #[error("IngestMetrics failed {source}")]
    IngestMetrics {
        /// Wrapped [`IngestMetricsError`]
        #[from]
        source: IngestMetricsError,
    },

    /// Error returned when a delta write fails.
    /// Ending Kafka offsets and counts for each partition are included to help identify the Kafka buffer that caused the write to fail.
    #[error(
    "Delta write failed: ending_offsets: {ending_offsets}, partition_counts: {partition_counts}, source: {source}"
    )]
    DeltaWriteFailed {
        /// Ending offsets for each partition that failed to be written to delta.
        ending_offsets: String,
        /// Message counts for each partition that failed to be written to delta.
        partition_counts: String,
        /// The underlying DataWriterError.
        source: DataWriterError,
    },

    /// Error returned when a message is received from Kafka that has already been processed.
    #[error(
        "Partition offset has already been processed - partition: {partition}, offset: {offset}"
    )]
    AlreadyProcessedPartitionOffset {
        /// The Kafka partition the message was received from
        partition: DataTypePartition,
        /// The Kafka offset of the message
        offset: DataTypeOffset,
    },

    /// Error returned when delta table is in an inconsistent state with the partition offsets being written.
    #[error("Delta table is in an inconsistent state: {0}")]
    InconsistentState(String),

    /// Error returned when a rebalance signal interrupts the run loop. This is handled by the runloop by resetting state, seeking the consumer and skipping the message.
    #[error("A rebalance signal exists while processing message")]
    RebalanceInterrupt,

    /// Error returned when the the offsets in delta log txn actions for assigned partitions have changed.
    #[error("Delta transaction log contains conflicting offsets for assigned partitions.")]
    ConflictingOffsets,

    /// Error returned when the delta schema has changed since the version used to write messages to the parquet buffer.
    #[error("Delta schema has changed and must be updated.")]
    DeltaSchemaChanged,

    /// Error returned if the committed Delta table version does not match the version specified by the commit attempt.
    #[error("Committed delta version {actual_version} does not match the version specified in the commit attempt {expected_version}"
    )]
    UnexpectedVersionMismatch {
        /// The version specified in the commit attempt
        expected_version: i64,
        /// The version returned after the commit
        actual_version: i64,
    },
    /// Error returned if unable to construct a deserializer
    #[error("Unable to construct a message deserializer, source: {source}")]
    UnableToCreateDeserializer {
        /// The underlying error.
        source: anyhow::Error,
    },
    /// Error returned if the message type should not be written to the delta table.
    #[error("Message does not match whitelist.")]
    FilteredMessageType,
}

/// Formats for message parsing
#[derive(Clone, Debug)]
pub enum MessageFormat {
    /// Parses messages as json and uses the inferred schema
    DefaultJson,

    /// Parses messages as json using the provided schema source. Will not use json schema files
    Json(SchemaSource),

    /// Parses avro messages using provided schema, schema registry or schema within file
    Avro(SchemaSource),

    #[cfg(feature = "akunaformats")]
    /// Parses protobuf messages. Currently only supports schema from SchemaRegistry
    Protobuf(SchemaSource),

    #[cfg(feature = "akunaformats")]
    /// Parses akuna aidl messages.
    Aidl(SchemaSource),
}

/// Source for schema
#[derive(Clone, Debug)]
pub enum SchemaSource {
    /// Use default behavior
    None,

    /// Use confluent schema registry url
    SchemaRegistry(Url),

    /// Use provided file for schema
    File(PathBuf),
}

#[derive(Clone, Debug)]
/// The enum to represent 'auto.offset.reset' options.
pub enum AutoOffsetReset {
    /// The "earliest" option. Messages will be ingested from the beginning of a partition on reset.
    Earliest,
    /// The "latest" option. Messages will be ingested from the end of a partition on reset.
    Latest,
}

impl AutoOffsetReset {
    /// The librdkafka config key used to specify an `auto.offset.reset` policy.
    pub const CONFIG_KEY: &'static str = "auto.offset.reset";
}

/// Options for configuring the behavior of the run loop executed by the [`start_ingest`] function.
#[derive(Clone, Debug)]
pub struct IngestOptions {
    /// The Kafka broker string to connect to.
    pub kafka_brokers: String,
    /// The Kafka consumer group id to set to allow for multiple consumers per topic.
    pub consumer_group_id: String,
    /// Unique per topic per environment. **Must** be the same for all processes that are part of a single job.
    /// It's used as a prefix for the `txn` actions to track messages offsets between partition/writers.
    pub app_id: String,
    /// Offsets to seek to before the ingestion. Creates new delta log version with `txn` actions
    /// to store the offsets for each partition in delta table.
    /// Note that `seek_offsets` is not the starting offsets, as such, then first ingested message
    /// will be `seek_offset + 1` or the next successive message in a partition.
    /// This configuration is only applied when offsets are not already stored in delta table.
    /// Note that if offsets are already exists in delta table but they're lower than provided
    /// then the error will be returned as this could break the data integrity. If one would want to skip
    /// the data and write from the later offsets then supplying new `app_id` is a safer approach.
    pub seek_offsets: Option<Vec<(DataTypePartition, DataTypeOffset)>>,
    /// The policy to start reading from if both `txn` and `seek_offsets` has no specified offset
    /// for the partition. Either "earliest" or "latest". The configuration is also applied to the
    /// librdkafka `auto.offset.reset` config.
    pub auto_offset_reset: AutoOffsetReset,
    /// Max desired latency from when a message is received to when it is written and
    /// committed to the target delta table (in seconds)
    pub allowed_latency: u64,
    /// Number of messages to buffer before writing a record batch.
    pub max_messages_per_batch: usize,
    /// Desired minimum number of compressed parquet bytes to buffer in memory
    /// before writing to storage and committing a transaction.
    pub min_bytes_per_file: usize,
    /// A list of transforms to apply to the message before writing to delta lake.
    pub transforms: IndexMap<String, String>,
    /// An optional dead letter table to write messages that fail deserialization, transformation or schema validation.
    pub dlq_table_uri: Option<String>,
    /// Transforms to apply to dead letters when writing to a delta table.
    pub dlq_transforms: IndexMap<String, String>,
    /// If `true` then application will write checkpoints on each 10th commit.
    pub write_checkpoints: bool,
    /// Additional properties to initialize the Kafka consumer with.
    pub additional_kafka_settings: Option<HashMap<String, String>>,
    /// A statsd endpoint to send statistics to.
    pub statsd_endpoint: Option<String>,

    /// Prometheus port to expose metrics
    pub prometheus_port: Option<usize>,
    /// Input format
    pub input_format: MessageFormat,
    /// Terminates when initial offsets are reached
    pub end_at_last_offsets: bool,
    /// The Base URI under which all the Delta Tables will be written.
    pub table_base_uri: String,

    /// A Regex string to filter message types to ingest.
    pub message_type_whitelist: Option<String>,

    /// When creating a new delta table, use the provided columns as partition columns, if they are available in the message_type.
    pub default_partition_columns: Vec<String>,

    /// The number of workers to spawn to handle deserialization of messages.
    pub deserialization_workers: usize,

    /// The number of messages to send to a deserialization worker at a time.
    pub worker_to_messages_multiplier: usize,

    /// The number of workers to spawn to handle transform and coerce.
    pub num_converting_workers: usize,
    /// The number of threads to use to conver json to arrow format per table.
    pub num_delta_write_threads: usize,
    /// The number of seconds to wait before flushing the table actors if no new messages are received.
    pub seconds_idle_kafka_read_before_flush: usize,
    /// The number of iterations to wait before performing housekeeping tasks. These include committing kafka offsets and flushing the delta tables.
    pub housekeeping_loop_iteration: usize,
    /// The number of versions to look back when reading the delta table to find the latest written offset. needs to be might enough to account for multiple writers. e.g. with 10 writers, at least 10 versions back would be needed.
    pub max_version_lookback: usize,
}

impl Default for IngestOptions {
    fn default() -> Self {
        IngestOptions {
            kafka_brokers: std::env::var("KAFKA_BROKERS").unwrap_or("localhost:9092".into()),
            consumer_group_id: "kafka_delta_ingest".to_string(),
            app_id: "kafka_delta_ingest".to_string(),
            seek_offsets: None,
            auto_offset_reset: AutoOffsetReset::Earliest,
            allowed_latency: 300,
            max_messages_per_batch: 5000,
            min_bytes_per_file: 134217728,
            transforms: IndexMap::new(),
            dlq_table_uri: None,
            dlq_transforms: IndexMap::new(),
            additional_kafka_settings: None,
            write_checkpoints: false,
            statsd_endpoint: None,
            prometheus_port: None,
            input_format: MessageFormat::DefaultJson,
            end_at_last_offsets: false,
            table_base_uri: "/".to_string(),
            message_type_whitelist: None,
            default_partition_columns: vec![],
            deserialization_workers: 10,
            worker_to_messages_multiplier: 1000,
            num_converting_workers: 10,
            num_delta_write_threads: 1,
            seconds_idle_kafka_read_before_flush: 180,
            housekeeping_loop_iteration: 1000,
            max_version_lookback: 10,
        }
    }
}

// Returns a list of keys from the URL. These should be delta tables.
/// This function is used to read the entries from the S3 bucket.
/// If an optional whitelist is provided, the entries are filtered based on the whitelist.
pub async fn read_entries_from_s3(
    url: &String,
    whitelist: &Option<Regex>,
) -> Result<Vec<String>, IngestError> {
    let region = if env::var("AWS_REGION").is_ok() {
        let region = env::var("AWS_REGION").unwrap();
        if region == "custom" {
            info!("Using custom region");
            match env::var("AWS_ENDPOINT_URL") {
                Ok(region_str) => Region::Custom {
                    name: region_str,
                    endpoint: env::var("AWS_ENDPOINT_URL").unwrap(),
                },
                Err(_) => Region::default(),
            }
        } else {
            Region::default()
        }
    } else {
        Region::default()
    };

    let client = S3Client::new(region);

    // regex match the bucket name from the url
    let bucket = url
        .replace("s3://", "")
        .split("/")
        .next()
        .unwrap()
        .to_string();
    let key = url
        .replace("s3://", "")
        .split("/")
        .skip(1)
        .collect::<Vec<&str>>()
        .join("/");

    let key = key + "/";
    let list_req = ListObjectsV2Request {
        bucket,
        prefix: Some(key.clone()),
        delimiter: Some("/".parse().unwrap()),
        ..Default::default()
    };
    trace!("Listing objects from S3: {:?}", list_req);
    let result = client.list_objects_v2(list_req).await;
    match result {
        Ok(result) => {
            trace!("Result Listing objects from S3: {:?}", result);
            let mut keys = vec![];
            if let Some(tables) = result.common_prefixes {
                trace!("Objects found in S3: {:?}", tables);
                tables.into_iter().for_each(|entry| {
                    if let Some(prefix) = entry.prefix {
                        let prefix = prefix.replace(&key, "").replace("/", "");
                        keys.push(prefix);
                    }
                });
            }
            trace!("Read tables from S3: {:?}", keys);
            // filter the list for the entries which match the whitelist, if provided.
            if let Some(re) = whitelist {
                keys.retain(|x| re.is_match(x));
            }
            Ok(keys)
        }
        Err(e) => {
            error!("Error listing objects from S3: {:?}", e);
            Err(IngestError::IoError {
                source: std::io::Error::new(std::io::ErrorKind::Other, e),
            })
        }
    }
}

/// This function is used to start the ingestion process using various async spawns to setup an Actor-like framework to handle the ingestion process.
pub async fn start_parallel_ingest(
    topic: String,
    table_base_uri: String,
    opts: IngestOptions,
    cancel_token: Option<Arc<CancellationToken>>,
) -> Result<(), IngestError> {
    let default_message_type = MessageType::from(&topic);
    let deserializer =
        match MessageDeserializerFactory::try_build(&opts.input_format, default_message_type) {
            Ok(deserializer) => deserializer,
            Err(e) => return Err(IngestError::UnableToCreateDeserializer { source: e }),
        };
    let kafka_reading_actor = parallel_processing::KafkaReadingActor::new(
        topic,
        table_base_uri,
        opts,
        deserializer,
        cancel_token,
    );
    let leaked_kafka_reading_actor = Box::leak(Box::new(kafka_reading_actor));
    leaked_kafka_reading_actor.start().await
}

fn build_metrics(
    opts: &IngestOptions,
) -> Result<Box<dyn IngestMetrics + Send + Sync>, IngestError> {
    let ingest_metrics: Box<dyn IngestMetrics + Send + Sync> = if opts.prometheus_port.is_some() {
        Box::new(PrometheusIngestMetrics::new(
            opts.prometheus_port.clone().unwrap() as u16,
        )?)
    } else {
        Box::new(StatsDIngestMetrics::new(opts.statsd_endpoint.clone())?)
    };
    Ok(ingest_metrics)
}

/// Error returned when message deserialization fails.
/// This is handled by the run loop, and the message is treated as a dead letter.
#[derive(thiserror::Error, Debug)]
pub(crate) enum MessageDeserializationError {
    #[error("Kafka message contained empty payload")]
    EmptyPayload,
    #[error("Kafka message deserialization failed")]
    JsonDeserialization { dead_letter: DeadLetter },
    #[error("Kafka message deserialization failed")]
    AvroDeserialization { dead_letter: DeadLetter },
    #[cfg(feature = "akunaformats")]
    #[error("Kafka message deserialization failed")]
    ProtobufDeserialization { dead_letter: DeadLetter },
    #[cfg(feature = "akunaformats")]
    #[error("AIDL message deserialization failed")]
    AIDLDeserialization { dead_letter: DeadLetter },
    #[cfg(feature = "akunaformats")]
    #[error("Akuna protobuf message deserialization failed")]
    AkunaProtobufDeserialization { dead_letter: DeadLetter },
}

// implement a from to convert from an akuna_deserialisation error to a MessageDeserializationError
#[cfg(feature = "akunaformats")]
impl From<akuna_deserializer::protobuf_decoder::ProtobufDeserializationError>
    for MessageDeserializationError
{
    fn from(e: akuna_deserializer::protobuf_decoder::ProtobufDeserializationError) -> Self {
        // convert the e.source_data to a &[u8]
        let bytes = e.source_data.iter().map(|x| *x).collect::<Vec<u8>>();
        MessageDeserializationError::AkunaProtobufDeserialization {
            dead_letter: DeadLetter::from_failed_deserialization(&bytes, e.reason),
        }
    }
}

/// Indicates whether a rebalance signal should simply skip the currently consumed message, or clear state and skip.
enum RebalanceAction {
    SkipMessage,
    ClearStateAndSkipMessage,
}

fn create_columns(template: &Value) -> Vec<StructField> {
    let mut r = vec![];
    for (name, value) in template.as_object().unwrap().iter() {
        let datatype = value_to_data_type(value);
        let field = StructField::new(name, datatype, true);
        r.push(field);
    }
    r
}

// Should we map from source proto type instead of json type?
fn value_to_data_type(value: &Value) -> DataType {
    match value {
        Value::Null => DataType::Primitive(PrimitiveType::String), // TODO: is this the right mapping?
        Value::Bool(_) => DataType::Primitive(PrimitiveType::Boolean),
        Value::Number(n) => n.as_i64().map_or_else(
            || {
                n.as_u64().map_or_else(
                    || {
                        n.as_f64()
                            .map_or(DataType::Primitive(PrimitiveType::String), |_| {
                                DataType::Primitive(PrimitiveType::Double)
                            })
                    },
                    |_| DataType::Primitive(PrimitiveType::Long),
                )
            },
            |_| DataType::Primitive(PrimitiveType::Long),
        ),
        Value::String(_) => DataType::Primitive(PrimitiveType::String),
        Value::Array(arr) => {
            let arr_type = match arr.get(0) {
                Some(v) => value_to_data_type(v),
                None => DataType::Primitive(PrimitiveType::String),
            };

            DataType::Array(Box::new(ArrayType::new(arr_type, true))) // TODO: is this right for nullability?
        }
        Value::Object(o) => {
            // check if this is a struct or a map

            #[cfg(feature = "akunaformats")]
            if o.contains_key(akuna_deserializer::protocol_decoder::MAP_MARKER_ENTRY) {
                // extract the key and value type informations
                return akunaformats::map_marker_entry_to_data_type(o);
            }
            // this is a struct
            DataType::Struct(Box::new(StructType::new(create_columns(&Value::Object(
                o.clone(),
            )))))
        }
    }
}

#[cfg(feature = "akunaformats")]
mod akunaformats {

    use akuna_deserializer::protocol_decoder::{MAP_MARKER_ENTRY, MAP_MARKER_ENTRY_KEY, MAP_MARKER_ENTRY_VALUE};
    use deltalake_core::kernel::{ArrayType, DataType, MapType, PrimitiveType, StructField, StructType};
    use log::error;
    use serde_json::{Map, Value};

    fn type_value_to_parquet_data_type(type_string: &Value) -> Option<DataType> {
        match type_string {
            Value::String(s) => match s.as_str() {
                "string" => Some(DataType::Primitive(PrimitiveType::String)),
                "int64" => Some(DataType::Primitive(PrimitiveType::Long)),
                "uint64" => Some(DataType::Primitive(PrimitiveType::Long)),
                "long" => Some(DataType::Primitive(PrimitiveType::Long)),
                "double" => Some(DataType::Primitive(PrimitiveType::Double)),
                "float" => Some(DataType::Primitive(PrimitiveType::Double)),
                "boolean" => Some(DataType::Primitive(PrimitiveType::Boolean)),
                "bool" => Some(DataType::Primitive(PrimitiveType::Boolean)),
                _ => {
                    error!("Unknown type string: {}", s);
                    Some(DataType::Primitive(PrimitiveType::String))
                }
            },
            Value::Object(type_map) => {
                #[cfg(feature = "akunaformats")]
                if type_map.contains_key(MAP_MARKER_ENTRY) {
                    return Some(map_marker_entry_to_data_type(type_map));
                }
                type_value_struct_to_parquet_data_type(type_map)
            }
            Value::Array(elems) => {
                // get first elem:
                let first = elems.get(0);
                match first {
                    Some(v) => {
                        let datatype = type_value_to_parquet_data_type(v);
                        Some(DataType::Array(Box::new(ArrayType::new(
                            datatype.unwrap(),
                            true,
                        ))))
                    }
                    None => {
                        error!("Array type has no elements");
                        None
                    }
                }
            }
            _ => {
                error!(
                    "Type string is not a string or a map. Instead: {:?}",
                    type_string
                );
                None
            }
        }
    }

    fn type_value_struct_to_parquet_data_type(type_map: &Map<String, Value>) -> Option<DataType> {
        let dt_list = type_map
            .iter()
            .map(|(k, v)| {
                let datatype = type_value_to_parquet_data_type(v);
                StructField::new(k, datatype.unwrap(), true)
            })
            .collect();
        Some(DataType::Struct(Box::new(StructType::new(dt_list))))
    }

    pub(crate) fn map_marker_entry_to_data_type(o: &Map<String, Value>) -> DataType {
        match o.get(MAP_MARKER_ENTRY).unwrap() {
            Value::Object(map) => {
                if map.contains_key(MAP_MARKER_ENTRY_KEY) {
                    let key_type = map
                        .get(MAP_MARKER_ENTRY_KEY)
                        .map_or(DataType::Primitive(PrimitiveType::String), |v| {
                            type_value_to_parquet_data_type(v).expect("Failed to map key type")
                        });
                    let value_type = map
                        .get(MAP_MARKER_ENTRY_VALUE)
                        .map_or(DataType::Primitive(PrimitiveType::String), |v| {
                            type_value_to_parquet_data_type(v).expect("Failed to map value type")
                        });
                    DataType::Map(Box::new(MapType::new(key_type, value_type, false)))
                } else {
                    // no key so this is an array
                    let value_type = map
                        .get(MAP_MARKER_ENTRY_VALUE)
                        .map_or(DataType::Primitive(PrimitiveType::String), |v| {
                            type_value_to_parquet_data_type(v).expect("Failed to map value type")
                        });
                    match &value_type {
                        DataType::Array(_) => {
                            DataType::Array(Box::new(ArrayType::new(value_type, false)))
                        }
                        _ => {
                            // unexpected value type
                            error!(
                                "Unexpected value type for map marker entry: {:?}",
                                value_type
                            );
                            DataType::Primitive(PrimitiveType::String)
                        }
                    }
                }
            }
            _ => {
                error!("Map marker entry is not an object");
                DataType::Primitive(PrimitiveType::String)
            }
        }
    }
}

/// Enum that represents a signal of an asynchronously received rebalance event that must be handled in the run loop.
/// Used to preserve correctness of messages stored in buffer after handling a rebalance event.
#[derive(Debug, PartialEq, Clone)]
enum RebalanceSignal {
    RebalanceRevoke,
    RebalanceAssign(Vec<DataTypePartition>),
}

/// Contains the partition to offset map for all partitions assigned to the consumer.
#[derive(Default)]
struct PartitionAssignment {
    assignment: HashMap<DataTypePartition, Option<DataTypeOffset>>,
}

impl PartitionAssignment {
    // /// Resets the [`PartitionAssignment`] with a new list of partitions.
    // /// Offsets are set as [`None`] for all partitions.
    // fn reset_with(&mut self, partitions: &[DataTypePartition]) {
    //     self.assignment.clear();
    //     for p in partitions {
    //         self.assignment.insert(*p, None);
    //     }
    // }
    /// Process a new assignment by clearing internal state. returns the list of partions added/removed.
    fn new_assignment(
        &mut self,
        partitions: &Vec<DataTypePartition>,
    ) -> (HashSet<DataTypePartition>, HashSet<DataTypePartition>) {
        let max_capacity = partitions.len() + self.assignment.capacity();
        let mut added = HashSet::with_capacity(max_capacity);
        let mut removed = HashSet::with_capacity(max_capacity);
        for e in partitions {
            if !self.assignment.contains_key(e) {
                added.insert(*e);
            }
        }
        for e in self.assignment.iter() {
            if !partitions.contains(&e.0) {
                removed.insert(*e.0);
            }
        }
        self.assignment.clear();
        for p in partitions {
            self.assignment.insert(*p, None);
        }
        (added, removed)
    }

    /// Updates the offsets for each partition stored in the [`PartitionAssignment`].
    fn update_offsets(&mut self, updated_offsets: &HashMap<DataTypePartition, DataTypeOffset>) {
        for (k, v) in updated_offsets {
            if let Some(entry) = self.assignment.get_mut(k) {
                *entry = Some(*v);
            }
        }
    }
}

/// Implements rdkafka [`ClientContext`] to handle rebalance events sent to the rdkafka [`Consumer`].
struct KafkaContext {
    rebalance_signal: Arc<RwLock<Option<RebalanceSignal>>>,
}

impl ClientContext for KafkaContext {}

impl ConsumerContext for KafkaContext {
    fn pre_rebalance(&self, rebalance: &Rebalance) {
        let rebalance_signal = self.rebalance_signal.clone();
        match rebalance {
            Rebalance::Revoke(_) => {
                info!("PRE_REBALANCE - Revoke");
                tokio::spawn(async move {
                    rebalance_signal
                        .write()
                        .await
                        .replace(RebalanceSignal::RebalanceRevoke);
                });
            }
            Rebalance::Assign(tpl) => {
                debug!("PRE_REBALANCE - Assign {:?}", tpl);
            }
            Rebalance::Error(e) => {
                panic!("PRE_REBALANCE - Unexpected Kafka error {:?}", e);
            }
        }
    }

    fn post_rebalance(&self, rebalance: &Rebalance) {
        let rebalance_signal = self.rebalance_signal.clone();
        match rebalance {
            Rebalance::Revoke(_) => {
                debug!("POST_REBALANCE - Revoke");
            }
            Rebalance::Assign(tpl) => {
                let partitions = partition_vec_from_topic_partition_list(tpl);
                info!("POST_REBALANCE - Assign {:?}", partitions);

                tokio::spawn(async move {
                    rebalance_signal
                        .write()
                        .await
                        .replace(RebalanceSignal::RebalanceAssign(partitions));
                });
            }
            Rebalance::Error(e) => {
                panic!("POST_REBALANCE - Unexpected Kafka error {:?}", e);
            }
        }
    }
}

/// Creates an rdkafka [`ClientConfig`] from the provided [`IngestOptions`].
fn kafka_client_config_from_options(opts: &IngestOptions) -> ClientConfig {
    let mut kafka_client_config = ClientConfig::new();
    if let Ok(cert_pem) = std::env::var("KAFKA_DELTA_INGEST_CERT") {
        kafka_client_config.set("ssl.certificate.pem", cert_pem);
    }
    if let Ok(key_pem) = std::env::var("KAFKA_DELTA_INGEST_KEY") {
        kafka_client_config.set("ssl.key.pem", key_pem);
    }
    if let Ok(scram_json) = std::env::var("KAFKA_DELTA_INGEST_SCRAM_JSON") {
        let value: Value = serde_json::from_str(scram_json.as_str())
            .expect("KAFKA_DELTA_INGEST_SCRAM_JSON should be valid JSON");

        let username = value["username"]
            .as_str()
            .expect("'username' must be present in KAFKA_DELTA_INGEST_SCRAM_JSON");
        let password = value["password"]
            .as_str()
            .expect("'password' must be present in KAFKA_DELTA_INGEST_SCRAM_JSON");

        kafka_client_config.set("sasl.username", username);
        kafka_client_config.set("sasl.password", password);
    }

    let auto_offset_reset = match opts.auto_offset_reset {
        AutoOffsetReset::Earliest => "earliest",
        AutoOffsetReset::Latest => "latest",
    };
    kafka_client_config.set(AutoOffsetReset::CONFIG_KEY, auto_offset_reset);

    if let Some(additional) = &opts.additional_kafka_settings {
        for (k, v) in additional.iter() {
            kafka_client_config.set(k, v);
        }
    }

    env::vars()
        .filter(|t| t.0.starts_with("KAFKA_SETTING_"))
        .map(|t| {
            (
                t.0.replace("KAFKA_SETTING_", "")
                    .replace("_", ".")
                    .to_lowercase(),
                t.1,
            )
        })
        .for_each(|t| {
            kafka_client_config.set(t.0, t.1);
            ()
        });

    kafka_client_config
        .set("bootstrap.servers", opts.kafka_brokers.clone())
        .set("group.id", opts.consumer_group_id.clone())
        .set("enable.auto.commit", "false");

    kafka_client_config
}

/// Creates a [`DeadLetterQueue`] to send broken messages to based on options.
async fn dead_letter_queue_from_options(
    opts: &IngestOptions,
) -> Result<Box<dyn DeadLetterQueue>, DeadLetterQueueError> {
    dlq_from_opts(DeadLetterQueueOptions {
        delta_table_uri: opts.dlq_table_uri.clone(),
        dead_letter_transforms: opts.dlq_transforms.clone(),
        write_checkpoints: opts.write_checkpoints,
    })
    .await
}

/// Creates a vec of partition numbers from a topic partition list.
fn partition_vec_from_topic_partition_list(
    topic_partition_list: &TopicPartitionList,
) -> Vec<DataTypePartition> {
    topic_partition_list
        .to_topic_map()
        .iter()
        .map(|((_, p), _)| *p)
        .collect()
}

// ignore deprecated warming
#[allow(deprecated)]
const EPOCH_DATE: NaiveDate = NaiveDate::from_ymd(1970, 1, 1);
