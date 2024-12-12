//! Binary used to run a kafka-delta-ingest command.
//!
//! # Summary
//! [`kafka_delta_ingest`] is a tool for writing data from a Kafka topic into a Delta Lake table.
//!
//! # Features
//!
//! * Apply simple transforms (using JMESPath queries or well known properties) to JSON before writing to Delta
//! * Write bad messages to a dead letter queue
//! * Send metrics to a Statsd endpoint, or expose via Prometheus
//! * Control the characteristics of output files written to Delta Lake by tuning buffer latency and file size parameters
//! * Automatically write Delta log checkpoints
//! * Passthrough of [librdkafka properties](https://github.com/edenhill/librdkafka/blob/master/CONFIGURATION.md) for additional Kafka configuration
//! * Start from explicit partition offsets.
//! * Write to any table URI supported by [`deltalake::storage::StorageBackend`]
//! * Support for Avro, JSON, and Protobuf message formats
//! * Creates table if it does not exist
//!
//! # Usage
//!
//! ```
//! // Start an ingest process to ingest data from `my-topic` into the Delta Lake table at `/my/table/uri`.
//! kafka-delta-ingest ingest --topic my-topic --table_base_uri /my/table/uri
//! ```
//!
//! ```
//! // List the available command line flags available for the `ingest` subcommand
//! kafka-delta-ingest ingest -h
//! ```

#![deny(warnings)]
#![deny(deprecated)]
#![deny(missing_docs)]

use clap::{Arg, ArgAction, ArgGroup, ArgMatches, Command};
use indexmap::IndexMap;
use kafka_delta_ingest::{
    repair::repair_and_optimize_table, start_parallel_ingest, AutoOffsetReset, DataTypeOffset,
    DataTypePartition, IngestOptions, MessageFormat, SchemaSource,
};
use log::{debug, error, info};
use std::collections::HashMap;
use std::io::prelude::*;
use std::path::PathBuf;
use std::process::ExitCode;
use std::str::FromStr;
#[cfg(not(target_env = "msvc"))]
use tikv_jemallocator::Jemalloc;

#[cfg(not(target_env = "msvc"))]
#[global_allocator]
static GLOBAL: Jemalloc = Jemalloc;

const MAX_SHUTDOWN_WAIT: u64 = 90;

fn main() -> ExitCode {
    deltalake_aws::register_handlers(None);

    // sleep for 15 seconds to allow magic trace attach
    // print the pid of the current process
    // println!("PID: {}", std::process::id());
    // std::thread::sleep(std::time::Duration::from_secs(15));

    #[cfg(feature = "sentry-ext")]
    {
        let _guard = std::env::var("SENTRY_DSN").ok().map(|dsn| {
            sentry::init((
                dsn,
                sentry::ClientOptions {
                    release: sentry::release_name!(),
                    ..Default::default()
                },
            ))
        });
    }

    let matches = build_app().get_matches();

    match matches.subcommand() {
        Some(("ingest", ingest_matches)) => {
            let app_id = ingest_matches
                .get_one::<String>("app_id")
                .unwrap()
                .to_string();

            init_logger(app_id.clone());

            let topic = ingest_matches
                .get_one::<String>("topic")
                .unwrap()
                .to_string();

            let kafka_brokers = ingest_matches
                .get_one::<String>("kafka")
                .unwrap()
                .to_string();
            let consumer_group_id = ingest_matches
                .get_one::<String>("consumer_group")
                .unwrap()
                .to_string();

            let seek_offsets = ingest_matches
                .get_one::<String>("seek_offsets")
                .map(|s| parse_seek_offsets(s));

            let auto_offset_reset = ingest_matches
                .get_one::<String>("auto_offset_reset")
                .unwrap()
                .to_string();

            let auto_offset_reset: AutoOffsetReset = match &auto_offset_reset as &str {
                "earliest" => AutoOffsetReset::Earliest,
                "latest" => AutoOffsetReset::Latest,
                unknown => panic!("Unknown auto_offset_reset {}", unknown),
            };

            let allowed_latency = ingest_matches.get_one::<u64>("allowed_latency").unwrap();
            let num_arrow_writer_threads = ingest_matches
                .get_one::<usize>("num_arrow_writer_threads")
                .unwrap();
            let max_version_lookback = ingest_matches
                .get_one::<usize>("max_version_lookback")
                .unwrap();
            let deserialization_workers = ingest_matches
                .get_one::<usize>("deserialization_workers")
                .unwrap();
            let max_messages_per_batch = ingest_matches
                .get_one::<usize>("max_messages_per_batch")
                .unwrap();
            let housekeeping_loop_iteration = ingest_matches
                .get_one::<usize>("housekeeping_loop_iteration")
                .unwrap();
            let min_bytes_per_file = ingest_matches
                .get_one::<usize>("min_bytes_per_file")
                .unwrap();

            let transforms: IndexMap<String, String> = ingest_matches
                .get_many::<String>("transform")
                .expect("Failed to parse transforms")
                .map(|t| split_into_transforms(t))
                .flatten()
                .map(|t| parse_transform(t).unwrap())
                .collect();

            info!("Transforms: {:?}", transforms);

            let dlq_table_location = ingest_matches
                .get_one::<String>("dlq_table_location")
                .map(|s| s.to_string());

            let dlq_transforms: IndexMap<String, String> = ingest_matches
                .get_many::<String>("dlq_transform")
                .expect("Failed to parse dlq transforms")
                .map(|t| split_into_transforms(t))
                .flatten()
                .map(|t| parse_transform(t).unwrap())
                .collect();

            let write_checkpoints = ingest_matches.get_flag("checkpoints");

            let additional_kafka_settings = ingest_matches
                .get_many::<String>("kafka_setting")
                .map(|k| k.map(|s| parse_kafka_property(s).unwrap()).collect());

            let statsd_endpoint = ingest_matches.get_one::<String>("statsd_endpoint").cloned();

            let prometheus_port = ingest_matches.get_one::<usize>("prometheus_port").cloned();
            if prometheus_port.is_some() && statsd_endpoint.is_some() {
                return Err(anyhow::anyhow!(
                    "Cannot specify both statsd_endpoint and prometheus_port"
                ))
                    .unwrap();
            }

            let end_at_last_offsets = ingest_matches.get_flag("end");

            // ingest_matches.get_flag("end")
            let format = convert_matches_to_message_format(ingest_matches).unwrap();

            let worker_to_messages_multiplier = ingest_matches
                .get_one::<usize>("worker_to_messages_multiplier")
                .unwrap();

            let seconds_idle_kafka_read_before_flush = ingest_matches
                .get_one::<usize>("seconds_idle_kafka_read_before_flush")
                .unwrap();

            let num_converting_workers = ingest_matches
                .get_one::<usize>("num_converting_workers")
                .unwrap();

            let default_partition_columns: Vec<String> = ingest_matches
                .get_many::<String>("partition_columns")
                .unwrap_or_default()
                .into_iter()
                .map(|s| s.to_string())
                .collect();

            let table_base_uri = ingest_matches
                .get_one::<String>("table_base_uri")
                .cloned()
                .unwrap();

            let message_type_whitelist = ingest_matches
                .get_one::<String>("message_type_whitelist")
                .map(|s| s.to_string());

            let options = IngestOptions {
                kafka_brokers,
                consumer_group_id,
                app_id,
                seek_offsets,
                auto_offset_reset,
                allowed_latency: *allowed_latency,
                max_messages_per_batch: *max_messages_per_batch,
                min_bytes_per_file: *min_bytes_per_file,
                transforms,
                dlq_table_uri: dlq_table_location,
                dlq_transforms,
                write_checkpoints,
                additional_kafka_settings,
                statsd_endpoint,
                prometheus_port,
                input_format: format,
                end_at_last_offsets,
                table_base_uri: table_base_uri.clone(),
                message_type_whitelist: message_type_whitelist,
                default_partition_columns: default_partition_columns,
                deserialization_workers: *deserialization_workers,
                worker_to_messages_multiplier: *worker_to_messages_multiplier,
                seconds_idle_kafka_read_before_flush: *seconds_idle_kafka_read_before_flush,
                num_converting_workers: *num_converting_workers,
                num_delta_write_threads: *num_arrow_writer_threads,
                housekeeping_loop_iteration: *housekeeping_loop_iteration,
                max_version_lookback: *max_version_lookback,
            };

            // let's build the tokio runtime
            let tokio_runtime = tokio::runtime::Builder::new_multi_thread()
                .enable_all()
                .build()
                .unwrap();

            // enter runtime
            let _guard = tokio_runtime.enter();

            let main_thread_result =
                tokio_runtime.block_on(start_parallel_ingest(topic, table_base_uri, options, None));
            let exit_code = match main_thread_result {
                Ok(_) => 0,
                Err(e) => {
                    error!("Ingest service exited with error {:?}", e);
                    1
                }
            };
            // wait for all tasks to finish
            info!("Waiting for all tasks to finish");
            let start_shutdown = std::time::Instant::now();

            while tokio_runtime.metrics().num_blocking_threads()
                != tokio_runtime.metrics().num_idle_blocking_threads()
                && start_shutdown.elapsed().as_secs() < MAX_SHUTDOWN_WAIT
            {
                debug!(
                    "{} blocking threads and {} idle threads before shutdown. . . ",
                    tokio_runtime.metrics().num_blocking_threads(),
                    tokio_runtime.metrics().num_idle_blocking_threads()
                );
                std::thread::sleep(std::time::Duration::from_secs(10));
            }
            info!("Shutting down tokio runtime");
            tokio_runtime.shutdown_timeout(std::time::Duration::from_secs(10));
            return ExitCode::from(exit_code);
        }
        Some(("repair", repair_matches)) => {
            let table_url = repair_matches
                .get_one::<String>("table_url")
                .cloned()
                .unwrap();

            let optimize_target_size = repair_matches
                .get_one::<usize>("optimize_target_size")
                .cloned()
                .unwrap();

            let optimize_partition = repair_matches
                .get_one::<String>("optimize_partition")
                .cloned();
            let optimize_predicate = repair_matches
                .get_one::<String>("optimize_partition_predicate")
                .cloned();
            let optimize_tuple = if let Some(optimize_partition) = optimize_partition {
                if optimize_predicate.is_some() {
                    Some((optimize_partition, optimize_predicate.unwrap()))
                } else {
                    None
                }
            } else {
                None
            };
            init_logger(format!("repair-{}", table_url.clone()));

            let tokio_runtime = tokio::runtime::Builder::new_multi_thread()
                .enable_all()
                .build()
                .unwrap();
            let _guard = tokio_runtime.enter();

            let main_thread_result = tokio_runtime.block_on(repair_and_optimize_table(
                table_url,
                optimize_target_size,
                optimize_tuple,
            ));
            let exit_code = match main_thread_result {
                Ok(_) => 0,
                Err(e) => {
                    error!("Repair failed with error {:?}", e);
                    1
                }
            };
            tokio_runtime.shutdown_timeout(std::time::Duration::from_secs(10));
            info!("Repairing table finished.");
            return ExitCode::from(exit_code);
        }
        _ => unreachable!(),
    }
}

fn split_into_transforms(raw_transforms: &String) -> Box<dyn Iterator<Item=String>> {
    Box::new(
        raw_transforms
            .split("^")
            .map(|e| String::from(e))
            .collect::<Vec<_>>()
            .into_iter(),
    )
}

fn to_schema_source(
    input: Option<&String>,
    disable_files: bool,
) -> Result<SchemaSource, SchemaSourceError> {
    match input {
        None => Ok(SchemaSource::None),
        Some(value) => {
            if value.is_empty() {
                return Ok(SchemaSource::None);
            }

            if !value.starts_with("http") {
                if disable_files {
                    return Ok(SchemaSource::None);
                }

                let p = PathBuf::from_str(value)?;
                if !p.exists() {
                    return Err(SchemaSourceError::FileNotFound {
                        file_name: (*value).clone(),
                    });
                }

                return Ok(SchemaSource::File(p));
            }

            Ok(SchemaSource::SchemaRegistry(url::Url::parse(value)?))
        }
    }
}

fn init_logger(app_id: String) {
    let app_id: &'static str = Box::leak(app_id.into_boxed_str());
    let log_level = std::env::var("RUST_LOG").unwrap_or_else(|_| "Info".to_string());
    // .ok()
    // .and_then(|l| LevelFilter::from_str(l.as_str()).ok())
    // .unwrap_or(log::LevelFilter::Info);

    let _ = env_logger::Builder::new()
        .format(move |buf, record| {
            let ts = buf.timestamp_micros();
            writeln!(
                buf,
                // "{}: {:?}({:?}) at {:?}::{:?}:{:?}: {}: {}",
                "{}: {}: {:?}({:?}): {}: {}",
                ts,
                app_id,
                std::thread::current().name(),
                std::thread::current().id(),
                // record.module_path(),
                // record.line(),
                // record.file(),
                record.level(),
                record.args()
            )
        })
        .parse_filters(&log_level)
        .try_init();
}

#[derive(thiserror::Error, Debug)]
#[error("'{value}' - Each transform argument must be colon delimited and match the pattern 'PROPERTY: SOURCE'"
)]
struct TransformSyntaxError {
    value: String,
}

#[derive(thiserror::Error, Debug)]
#[error("'{value}' - Each Kafka setting must be delimited by an '=' and match the pattern 'PROPERTY_NAME=PROPERTY_VALUE'"
)]
struct KafkaPropertySyntaxError {
    value: String,
}

/// Errors returned by [`to_schema_source`] function.
#[derive(thiserror::Error, Debug)]
enum SchemaSourceError {
    /// Wrapped [`core::convert::Infallible`]
    #[error("Invalid file path: {source}")]
    InvalidPath {
        #[from]
        source: core::convert::Infallible,
    },
    /// Wrapped [`url::ParseError`]
    #[error("Invalid url: {source}")]
    InvalidUrl {
        #[from]
        source: url::ParseError,
    },
    #[error("File not found error: {file_name}")]
    FileNotFound { file_name: String },
}

fn parse_kafka_property(val: &str) -> Result<(String, String), KafkaPropertySyntaxError> {
    parse_tuple(String::from(val), "=").map_err(|s| KafkaPropertySyntaxError { value: s })
}

fn parse_transform(val: String) -> Result<(String, String), TransformSyntaxError> {
    parse_tuple(val, ":").map_err(|s| TransformSyntaxError { value: s })
}

// parse argument as a duple and let clap format the error in case of invalid syntax.
// this function is used both as a validator in the clap config, and to extract the program
// arguments.
fn parse_tuple(val: String, delimiter: &str) -> Result<(String, String), String> {
    let splits: Vec<&str> = val.splitn(2, delimiter).map(|s| s.trim()).collect();

    match splits.len() {
        2 => {
            let tuple: (String, String) = (splits[0].to_owned(), splits[1].to_owned());
            Ok(tuple)
        }
        _ => Err(val.to_string()),
    }
}

fn parse_seek_offsets(val: &str) -> Vec<(DataTypePartition, DataTypeOffset)> {
    let map: HashMap<String, DataTypeOffset> =
        serde_json::from_str(val).expect("Cannot parse seek offsets");

    let mut list: Vec<(DataTypePartition, DataTypeOffset)> = map
        .iter()
        .map(|(p, o)| (p.parse::<DataTypePartition>().unwrap(), *o))
        .collect();

    list.sort_by(|a, b| a.0.cmp(&b.0));
    list
}

fn build_app() -> Command {
    Command::new("kafka-delta-ingest")
        .version(env!["CARGO_PKG_VERSION"])
        .about("Daemon for ingesting messages from Kafka and writing them to a Delta table")
        .subcommand(
            Command::new("repair")
                .about("Repair a Delta table")
                .arg(Arg::new("table_url")
                    .long("table_url")
                    .env("TABLE_URL")
                    .help("The URL under which the table exists.")
                    .required(true))
                .arg(Arg::new("optimize_target_size")
                    .long("optimize_target_size")
                    .env("OPTIMIZE_TARGET_SIZE")
                    .help("The target size for the optimized files")
                    .default_value("100000000")
                    .value_parser(clap::value_parser!(usize))
                    .required(false))
                .arg(Arg::new("optimize_partition")
                    .long("optimize_partition")
                    .env("OPTIMIZE_PARTITION")
                    .help("The partition to optimize")
                    .required(false))
                .arg(Arg::new("optimize_partition_predicate")
                    .long("optimize_partition_predicate")
                    .env("OPTIMIZE_PARTITION_PREDICATE")
                    .help("The predicate for the partition to optimize")
                    .required(false))
        )
        .subcommand(
            Command::new("ingest")
                .about("Starts a stream that consumes from a Kafka topic and writes to a Delta table")
                .arg(Arg::new("table_base_uri")
                    .long("table_base_uri")
                    .env("TABLE_BASE_URI")
                    .help("The Base URI under which all tables will be created/written to.")
                    .required(true))
                .arg(Arg::new("message_type_whitelist")
                    .long("message_type_whitelist")
                    .env("MESSAGE_TYPE_WHITELIST")
                    .help("A Regex which will be used to filter messages based on their type. Only messages that match the regex will be ingested. If this field is not provided all message types will be saved.")
                    .required(false))
                .arg(Arg::new("topic")
                    .env("KAFKA_TOPIC")
                    .help("The Kafka topic to stream from")
                    .long("topic")
                    .required(true))
                .arg(Arg::new("kafka")
                    .short('k')
                    .long("kafka")
                    .env("KAFKA_BROKERS")
                    .help("Kafka broker connection string to use")
                    .default_value("localhost:9092"))
                .arg(Arg::new("consumer_group")
                    .short('g')
                    .long("consumer_group")
                    .env("KAFKA_CONSUMER_GROUP")
                    .help("Consumer group to use when subscribing to Kafka topics")
                    .default_value("kafka_delta_ingest"))
                .arg(Arg::new("app_id")
                    .short('a')
                    .long("app_id")
                    .env("APP_ID")
                    .help("App ID to use when writing to Delta")
                    .default_value("kafka_delta_ingest"))
                .arg(Arg::new("seek_offsets")
                    .long("seek_offsets")
                    .env("KAFKA_SEEK_OFFSETS")
                    .help(r#"Only useful when offsets are not already stored in the delta table. A JSON string specifying the partition offset map as the starting point for ingestion. This is *seeking* rather than _starting_ offsets. The first ingested message would be (seek_offset + 1). Ex: {"0":123, "1":321}"#))
                .arg(Arg::new("auto_offset_reset")
                    .short('o')
                    .long("auto_offset_reset")
                    .env("KAFKA_AUTO_OFFSET_RESET")
                    .help(r#"The default offset reset policy, which is either 'earliest' or 'latest'.
The configuration is applied when offsets are not found in delta table or not specified with 'seek_offsets'. This also overrides the kafka consumer's 'auto.offset.reset' config."#)
                    .default_value("earliest"))
                .arg(Arg::new("allowed_latency")
                    .short('l')
                    .long("allowed_latency")
                    .env("ALLOWED_LATENCY")
                    .help("The allowed latency (in seconds) from the time a message is consumed to when it should be written to Delta.")
                    .default_value("300")
                    .value_parser(clap::value_parser!(u64)))
                .arg(Arg::new("max_messages_per_batch")
                    .short('m')
                    .long("max_messages_per_batch")
                    .env("MAX_MESSAGES_PER_BATCH")
                    .help("The maximum number of rows allowed in a parquet batch. This shoulid be the approximate number of bytes described by MIN_BYTES_PER_FILE")
                    .default_value("1000000")
                    .value_parser(clap::value_parser!(usize)))
                .arg(Arg::new("housekeeping_loop_iteration")
                    .long("housekeeping_loop_iteration")
                    .env("HOUSEKEEPING_LOOP_ITERATION")
                    .help("The number of message receive loops to execute before performing housekeeping tasks.")
                    .default_value("5000")
                    .value_parser(clap::value_parser!(usize)))
                .arg(Arg::new("deserialization_workers")
                    .long("deserialization_workers")
                    .env("DESERIALIZATION_WORKERS")
                    .help("The number of workers to spawn to process deserializing messages")
                    .default_value("10")
                    .value_parser(clap::value_parser!(usize)))
                .arg(Arg::new("num_arrow_writer_threads")
                    .long("num_arrow_writer_threads")
                    .env("NUM_ARROW_WRITER_THREADS")
                    .help("The number of threads to spawn to process converting JSON into arrow recordbatches")
                    .default_value("4")
                    .value_parser(clap::value_parser!(usize)))
                .arg(Arg::new("max_version_lookback")
                    .long("max_version_lookback")
                    .env("MAX_VERSION_LOOKBACK")
                    .help("The number of version to go back on a delta table to find the latest offset for a partition. Set this higher for more parallel workers.")
                    .default_value("5")
                    .value_parser(clap::value_parser!(usize)))
                .arg(Arg::new("min_bytes_per_file")
                    .short('b')
                    .long("min_bytes_per_file")
                    .env("MIN_BYTES_PER_FILE")
                    .help("The target minimum file size (in bytes) for each Delta file. File size may be smaller than this value if ALLOWED_LATENCY does not allow enough time to accumulate the specified number of bytes.")
                    .default_value("134217728")
                    .value_parser(clap::value_parser!(usize)))
                .arg(Arg::new("partition_columns")
                    .long("partition_columns")
                    .env("PARTITION_COLUMNS")
                    .value_delimiter(',')
                    .action(ArgAction::Set)
                    .help("When creating a new delta table, use the provided list of columns for the partitioning. The list if there is more than one should be comma seperated."))
                .arg(Arg::new("transform")
                    .short('t')
                    .long("transform")
                    .env("TRANSFORMS")
                    .action(ArgAction::Append)
                    .help(
                        r#"A list of transforms to apply to each Kafka message. Each transform should follow the pattern:
"PROPERTY: SOURCE". For example:

... -t 'modified_date: substr(modified,`0`,`10`)' 'kafka_offset: kafka.offset'

Valid values for SOURCE come in two flavors: (1) JMESPath query expressions and (2) well known
Kafka metadata properties. Both are demonstrated in the example above.

The first SOURCE extracts a substring from the "modified" property of the JSON value, skipping 0
characters and taking 10. the transform assigns the result to "modified_date" (the PROPERTY).
You can read about JMESPath syntax in https://jmespath.org/specification.html. In addition to the
built-in JMESPath functions, Kafka Delta Ingest adds the custom `substr` function.

The second SOURCE represents the well-known Kafka "offset" property. Kafka Delta Ingest supports
the following well-known Kafka metadata properties:

* kafka.offset
* kafka.partition
* kafka.topic
* kafka.timestamp
"#))
                .arg(Arg::new("dlq_table_location")
                    .long("dlq_table_location")
                    .env("DLQ_TABLE_LOCATION")
                    .required(false)
                    .help("Optional table to write unprocessable entities to"))
                .arg(Arg::new("dlq_transform")
                    .long("dlq_transform")
                    .env("DLQ_TRANSFORMS")
                    .required(false)
                    .action(ArgAction::Append)
                    .help("Transforms to apply before writing unprocessable entities to the dlq_location"))
                .arg(Arg::new("checkpoints")
                    .short('c')
                    .long("checkpoints")
                    .env("WRITE_CHECKPOINTS")
                    .action(ArgAction::SetTrue)
                    .help("If set then kafka-delta-ingest will write checkpoints on every 10th commit"))
                .arg(Arg::new("kafka_setting")
                    .short('K')
                    .long("kafka_setting")
                    .action(ArgAction::Append)
                    .help(r#"A list of additional settings to include when creating the Kafka consumer.

This can be used to provide TLS configuration as in:

... -K "security.protocol=SSL" "ssl.certificate.location=kafka.crt" "ssl.key.location=kafka.key""#))
                .arg(Arg::new("statsd_endpoint")
                    .short('s')
                    .long("statsd_endpoint")
                    .env("STATSD_ENDPOINT")
                    .help("Statsd endpoint for sending stats"))
                .arg(Arg::new("seconds_idle_kafka_read_before_flush")
                    .long("seconds_idle_kafka_read_before_flush")
                    .env("SECONDS_IDLE_KAFKA_READ_BEFORE_FLUSH")
                    .default_value("180")
                    .help("The number of seconds to wait before flushing the table state when no messages are being read.")
                    .value_parser(clap::value_parser!(usize)))
                .arg(Arg::new("worker_to_messages_multiplier")
                    .long("worker_to_messages_multiplier")
                    .env("WORKER_TO_MESSAGES_MULTIPLIER")
                    .default_value("10000")
                    .help("The multiplier for how many messages to send to each deserialisation worker per loop iteration.")
                    .value_parser(clap::value_parser!(usize)))
                .arg(Arg::new("num_converting_workers")
                    .long("num_converting_workers")
                    .env("NUM_CONVERTING_WORKERS")
                    .default_value("4")
                    .help("The Number of Converting workers per type to create. These workers will apply the transform and coerce. The number of workers should be balanced against the deserialisation workers to achieve max throughput.")
                    .value_parser(clap::value_parser!(usize)))
                .arg(Arg::new("prometheus_port")
                    .long("prometheus_port")
                    .env("PROMETHEUS_PORT")
                    .help("Port where prometheus stats will be exposed")
                    .value_parser(clap::value_parser!(usize)))
                .arg(Arg::new("json")
                    .required(false)
                    .long("json")
                    .env("JSON_REGISTRY")
                    .help("Schema registry endpoint, local path, or empty string"))
                .arg(Arg::new("protobuf")
                    .required(false)
                    .long("protobuf")
                    .env("PROTOBUF_REGISTRY")
                    .help("Schema registry endpoint"))
                .arg(Arg::new("avro")
                    .long("avro")
                    .env("AVRO_REGISTRY")
                    .required(false)
                    .help("Schema registry endpoint, local path, or empty string"))
                .arg(Arg::new("aidl")
                    .long("aidl")
                    .env("AIDL_REGISTRY")
                    .required(false)
                    .help("AIDL rest registry endpoint, local path, or empty string"))
                .group(ArgGroup::new("format")
                    .args(["json", "avro", "protobuf", "aidl"])
                    .required(false))
                .arg(Arg::new("end")
                    .short('e')
                    .long("ends_at_latest_offsets")
                    .env("TURN_OFF_AT_KAFKA_TOPIC_END")
                    .required(false)
                    .num_args(0)
                    .action(ArgAction::SetTrue)
                    .help(""))
        )
        .arg_required_else_help(true)
}

/// Parse a range string to a vector of usize
///
/// # Arguments
/// - range_str: &str - the range string to parse
///
/// # Returns
/// - Result<Vec<usize>, anyhow::Error> - the parsed range
///
/// # Example
/// ```
/// use notpu::utils::parse_range_usize;
///
/// let range = parse_range_usize("0-3").unwrap();
/// assert_eq!(range, vec![0, 1, 2]);
///
/// let range = parse_range_usize("0,1,2,3").unwrap();
/// assert_eq!(range, vec![0, 1, 2, 3]);
/// ```
pub fn parse_range_usize(range_str: &str) -> anyhow::Result<Vec<usize>> {
    // parse both format: 0-3 or 0,1,2,3
    if range_str.contains('-') {
        let mut range = range_str.split('-');
        let start = range
            .next()
            .ok_or_else(|| anyhow::anyhow!("Invalid range"))?;
        let end = range
            .next()
            .ok_or_else(|| anyhow::anyhow!("Invalid range"))?;
        let start = start
            .parse::<usize>()
            .map_err(|_| anyhow::anyhow!("Invalid range"))?;
        let end = end
            .parse::<usize>()
            .map_err(|_| anyhow::anyhow!("Invalid range"))?;

        Ok((start..end).collect::<Vec<usize>>())
    } else {
        let range = range_str
            .split(',')
            .map(|s| {
                s.parse::<usize>()
                    .map_err(|_| anyhow::anyhow!("Invalid range"))
            })
            .collect::<Result<Vec<usize>, _>>()?;
        Ok(range)
    }
}

fn convert_matches_to_message_format(
    ingest_matches: &ArgMatches,
) -> Result<MessageFormat, SchemaSourceError> {
    if !ingest_matches.contains_id("format") {
        Ok(MessageFormat::DefaultJson)
    } else if ingest_matches.contains_id("avro") {
        to_schema_source(ingest_matches.get_one::<String>("avro"), false).map(MessageFormat::Avro)
    } else if ingest_matches.contains_id("protobuf") {
        if cfg!(feature = "akunaformats") {
            #[cfg(feature = "akunaformats")]
            return to_schema_source(ingest_matches.get_one::<String>("protobuf"), false)
                .map(MessageFormat::Protobuf);
            #[allow(unreachable_code)]
            Err(SchemaSourceError::FileNotFound {
                file_name: "Protobuf not enabled in this build".to_string(),
            })
        } else {
            Err(SchemaSourceError::FileNotFound {
                file_name: "Protobuf not enabled in this build".to_string(),
            })
        }
    } else if ingest_matches.contains_id("aidl") {
        if cfg!(feature = "akunaformats") {
            #[cfg(feature = "akunaformats")]
            return to_schema_source(ingest_matches.get_one::<String>("aidl"), false)
                .map(MessageFormat::Aidl);
            #[allow(unreachable_code)]
            Err(SchemaSourceError::FileNotFound {
                file_name: "Aidl not enabled in this build".to_string(),
            })
        } else {
            Err(SchemaSourceError::FileNotFound {
                file_name: "Aidl not enabled in this build".to_string(),
            })
        }
    } else {
        to_schema_source(ingest_matches.get_one::<String>("json"), true).map(MessageFormat::Json)
    }
}

#[cfg(test)]
mod test {
    use clap::ArgMatches;
    use kafka_delta_ingest::{MessageFormat, SchemaSource};

    use crate::{
        build_app, convert_matches_to_message_format, parse_seek_offsets, SchemaSourceError,
    };

    const SCHEMA_REGISTRY_ADDRESS: &str = "http://localhost:8081";

    #[test]
    fn parse_seek_offsets_test() {
        let parsed = parse_seek_offsets(r#"{"0":10,"2":12,"1":13}"#);
        assert_eq!(parsed, vec![(0, 10), (1, 13), (2, 12)]);
    }

    #[test]
    fn get_json_argument() {
        let schema_registry_url: url::Url = url::Url::parse(SCHEMA_REGISTRY_ADDRESS).unwrap();
        assert!(matches!(
            get_subcommand_matches(vec!["--json", ""]).unwrap(),
            MessageFormat::Json(SchemaSource::None)
        ));
        assert!(matches!(
            get_subcommand_matches(vec!["--json", "test"]).unwrap(),
            MessageFormat::Json(SchemaSource::None)
        ));

        match get_subcommand_matches(vec!["--json", SCHEMA_REGISTRY_ADDRESS]).unwrap() {
            MessageFormat::Json(SchemaSource::SchemaRegistry(registry_url)) => {
                assert_eq!(registry_url, schema_registry_url);
            }
            _ => panic!("invalid message format"),
        }

        assert!(matches!(
            get_subcommand_matches(vec!["--json", "http::a//"]),
            Err(SchemaSourceError::InvalidUrl { .. })
        ));
    }

    #[test]
    fn get_avro_argument() {
        let schema_registry_url: url::Url = url::Url::parse(SCHEMA_REGISTRY_ADDRESS).unwrap();
        assert!(matches!(
            get_subcommand_matches(vec!["--avro", ""]).unwrap(),
            MessageFormat::Avro(SchemaSource::None)
        ));

        match get_subcommand_matches(vec!["--avro", "tests/data/default_schema.avro"]).unwrap() {
            MessageFormat::Avro(SchemaSource::File(file_name)) => {
                assert_eq!(
                    file_name.to_str().unwrap(),
                    "tests/data/default_schema.avro"
                );
            }
            _ => panic!("invalid message format"),
        }

        assert!(matches!(
            get_subcommand_matches(vec!["--avro", "this_file_does_not_exist"]),
            Err(SchemaSourceError::FileNotFound { .. })
        ));

        match get_subcommand_matches(vec!["--avro", SCHEMA_REGISTRY_ADDRESS]).unwrap() {
            MessageFormat::Avro(SchemaSource::SchemaRegistry(registry_url)) => {
                assert_eq!(registry_url, schema_registry_url)
                // assert_eq!(registry_url, SCHEMA_REGISTRY_ADDRESS);
            }
            _ => panic!("invalid message format"),
        }

        assert!(matches!(
            get_subcommand_matches(vec!["--avro", "http::a//"]),
            Err(SchemaSourceError::InvalidUrl { .. })
        ));
    }

    #[test]
    fn get_end_at_last_offset() {
        let values = ["-e", "--ends_at_latest_offsets"];
        for value in values {
            let subcommand = get_subcommand_matches_raw(vec![value]);
            assert!(subcommand.contains_id("end"));
            assert!(subcommand.get_flag("end"));
        }

        let subcommand = get_subcommand_matches_raw(vec![]);
        assert!(subcommand.contains_id("end"));
        assert!(!subcommand.get_flag("end"));
    }

    fn get_subcommand_matches(args: Vec<&str>) -> Result<MessageFormat, SchemaSourceError> {
        let arg_matches = get_subcommand_matches_raw(args);
        convert_matches_to_message_format(&arg_matches)
    }

    fn get_subcommand_matches_raw(args: Vec<&str>) -> ArgMatches {
        let base_args = vec![
            "app",
            "ingest",
            "--topic",
            "web_requests",
            "--table_base_uri",
            "./tests/data/web_requests",
            "-a",
            "test",
        ];
        let try_matches = build_app().try_get_matches_from([base_args, args].concat());
        let mut matches = try_matches.unwrap();
        let (_, subcommand) = matches.remove_subcommand().unwrap();
        subcommand
    }
}
