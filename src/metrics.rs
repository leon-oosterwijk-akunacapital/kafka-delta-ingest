use dipstick::*;
use log::{error, info};
use metrics::{counter, gauge, histogram, Histogram};
use metrics_exporter_prometheus::PrometheusBuilder;
use metrics_util::MetricKindMask;
use std::collections::HashMap;
use std::convert::TryInto;
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::sync::Once;
use std::time::{Duration, Instant};

static INIT: Once = Once::new();

/// The environment variable used to specify how many metrics should be written to the metrics queue before flushing to statsd.
const METRICS_INPUT_QUEUE_SIZE_VAR_NAME: &str = "KDI_METRICS_INPUT_QUEUE_SIZE";
/// The environment variable used to specify a prefix for metrics.
const METRICS_PREFIX_VAR_NAME: &str = "KDI_METRICS_PREFIX";

/// The default input queue size for sending metrics to statsd.
const DEFAULT_INPUT_QUEUE_SIZE: usize = 100;

/// Error returned when there is a failure in [`IngestMetrics`].
#[derive(thiserror::Error, Debug)]
pub enum IngestMetricsError {
    /// Error returned when the environment variable provided for METRICS_INPUT_QUEUE_SIZE could not be parsed
    #[error("Could not parse {0} provided in METRICS_INPUT_QUEUE_SIZE env variable")]
    InvalidMetricsInputQueueSize(String),
}

pub(crate) trait IngestMetrics {
    /// increments a counter for message deserialized
    fn n_message_deserialized(&mut self, label: &str, count: i64) {
        self.record_stat(StatType::MessageDeserialized, count, Some(label));
    }

    /// increments a counter for message deserialization failed
    fn message_deserialization_failed(&mut self) {
        self.record_one(StatType::MessageDeserializationFailed, None);
    }

    /// increments a counter for message deserialized
    fn message_transform_failed(&mut self, label: &str, count: i64) {
        self.record_stat(StatType::MessageTransformFailed, count, Some(label));
    }

    /// increments a counter for message transform failed
    fn message_skipped(&mut self, label: &str) {
        self.record_one(StatType::MessageSkipped, Some(label));
    }

    /// increments a counter for record batch completed.
    /// records a guage stat for buffered record batches.
    /// records a timer stat for record batch write duration.
    fn batch_completed(
        &mut self,
        buffered_record_batch_count: usize,
        timer: &Instant,
        label: &str,
    ) {
        let duration = timer.elapsed().as_millis() as i64;
        self.record_one(StatType::RecordBatchCompleted, Some(label));
        self.record_stat(
            StatType::BufferedRecordBatches,
            buffered_record_batch_count as i64,
            Some(label),
        );
        self.record_stat(StatType::RecordBatchWriteDuration, duration, Some(label));
    }

    /// increments a counter for record batch completed.
    /// records a guage stat for buffered record batches.
    /// records a timer stat for record batch write duration.
    fn batch_started(&mut self, label: &str) {
        self.record_one(StatType::RecordBatchStarted, Some(label));
    }

    /// increments a counter for delta write started
    fn delta_write_started(&mut self, label: &str) {
        self.record_one(StatType::DeltaWriteStarted, Some(label));
    }

    /// increments a counter for delta write started.
    /// records a timer stat for delta write duration.
    fn delta_write_completed(&mut self, timer: &Instant, label: &str) {
        let duration = timer.elapsed().as_millis() as i64;
        self.record_one(StatType::DeltaWriteCompleted, Some(label));
        self.record_stat(StatType::DeltaWriteDuration, duration, Some(label));
    }

    /// records a timer stat for deserialize duration.
    fn deserialize_completed(&mut self, timer: &Instant) {
        let duration = timer.elapsed().as_millis() as i64;
        self.record_stat(StatType::MessageDeserializedDuration, duration, None);
    }
    /// records a timer stat for deserialize duration.
    fn transform_completed(&mut self, timer: &Instant, label: &str) {
        let duration = timer.elapsed().as_millis() as i64;
        self.record_stat(StatType::TransformDuration, duration, Some(label));
    }

    /// increments a counter for delta write failed.
    fn delta_write_failed(&mut self, label: &str) {
        self.record_one(StatType::DeltaWriteFailed, Some(label));
    }

    /// Records a count of 1 for the metric.
    fn record_one(&mut self, stat_type: StatType, label: Option<&str>) {
        self.record_stat(stat_type, 1, label);
    }

    /// Records a metric for the given [`StatType`] with the given value.
    fn record_stat(&mut self, stat_type: StatType, val: i64, label: Option<&str>) {
        match stat_type {
            // timers
            StatType::RecordBatchWriteDuration
            | StatType::DeltaWriteDuration
            | StatType::MessageDeserializedDuration
            | StatType::TransformDuration => {
                self.handle_timer(stat_type, val, label);
            }

            // gauges
            StatType::BufferedRecordBatches | StatType::DeltaWriteNumPartitions => {
                self.handle_gauge(stat_type, val, label);
            }

            // counters
            _ => {
                self.handle_counter(stat_type, val, label);
            }
        }
    }

    /// Records a timer metric for the given [`StatType`].
    fn handle_timer(&mut self, stat_type: StatType, duration_us: i64, label: Option<&str>);

    /// Records a gauge metric for the given [`StatType`].
    fn handle_gauge(&self, stat_type: StatType, count: i64, label: Option<&str>);
    /// Records a counter metric for the given [`StatType`].
    fn handle_counter(&self, stat_type: StatType, count: i64, label: Option<&str>);
}

/// Wraps a [`dipstick::queue::InputQueueScope`] to provide a higher level API for recording metrics.
#[derive(Clone)]
pub(crate) struct StatsDIngestMetrics {
    metrics: InputQueueScope,
}

impl StatsDIngestMetrics {
    /// Creates an instance of [`IngestMetrics`] for sending metrics to statsd.
    pub(crate) fn new(statsd_endpoint: Option<String>) -> Result<Self, IngestMetricsError> {
        let metrics = create_queue(statsd_endpoint)?;

        Ok(Self { metrics })
    }
}

impl IngestMetrics for StatsDIngestMetrics {
    fn handle_timer(&mut self, stat_type: StatType, duration_us: i64, label: Option<&str>) {
        let stat_string = stat_type.to_string();
        // if label is Some concatenate its contents with stat_string
        let stat_string = match label {
            Some(label) => format!("{}.{}", stat_string, label),
            None => stat_string,
        };
        if let Ok(duration) = duration_us.try_into() {
            self.metrics
                .timer(stat_string.as_str())
                .interval_us(duration);
        } else {
            error!("Failed to report timer to statsd with an i64 that couldn't fit into u64.");
        }
    }

    fn handle_gauge(&self, stat_type: StatType, count: i64, label: Option<&str>) {
        let stat_string = stat_type.to_string();
        let stat_string = match label {
            Some(label) => format!("{}.{}", stat_string, label),
            None => stat_string,
        };
        let key = stat_string.as_str();

        self.metrics.gauge(key).value(count);
    }

    fn handle_counter(&self, stat_type: StatType, count: i64, label: Option<&str>) {
        let stat_string = stat_type.to_string();
        let stat_string = match label {
            Some(label) => format!("{}.{}", stat_string, label),
            None => stat_string,
        };
        let key = stat_string.as_str();

        let sized_count: usize = count.try_into().expect("Could not convert to usize");

        self.metrics.counter(key).count(sized_count);
    }
}

/// Wraps a prometheus_endpoint to provide a higher level API for recording metrics.
#[derive(Clone)]
pub(crate) struct PrometheusIngestMetrics {
    metric_handles: HashMap<String, Histogram>,
}

impl PrometheusIngestMetrics {
    /// Creates an instance of [`IngestMetrics`] for allowing prometheus endpoint scrapes.
    pub(crate) fn new(port: u16) -> Result<Self, IngestMetricsError> {
        // this socket listerning code should only execute once. make sure its in a static
        // block or something.
        INIT.call_once(|| {
            info!("Starting Prometheus Listener on port [{}]", port);
            let addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::UNSPECIFIED), port);
            let builder = PrometheusBuilder::new();
            builder
                .idle_timeout(
                    MetricKindMask::COUNTER | MetricKindMask::HISTOGRAM,
                    Some(Duration::from_secs(1800)),
                )
                .with_http_listener(addr)
                .install()
                .expect("failed to install Prometheus recorder");
        });

        Ok(Self {
            metric_handles: HashMap::new(),
        })
    }
}

const MSG_TYPE: &str = "message_type";

impl IngestMetrics for PrometheusIngestMetrics {
    fn handle_timer(&mut self, stat_type: StatType, duration_us: i64, label: Option<&str>) {
        // gets the histogram from the hashmap metric_hands or else registers a new histogram.
        let stat_handle_string = match label {
            Some(label) => format!("{}.{}", stat_type.to_string(), label),
            None => stat_type.to_string(),
        };

        let histogram = self
            .metric_handles
            .entry(stat_handle_string)
            .or_insert_with(|| histogram!(stat_type.to_string()));

        histogram.record(duration_us as f64);
    }

    fn handle_gauge(&self, stat_type: StatType, count: i64, label: Option<&str>) {
        let stat_string = stat_type.to_string();
        match label {
            Some(label) => gauge!(stat_string, MSG_TYPE => format!("{}", label)).set(count as f64),
            None => gauge!(stat_string).set(count as f64),
        };
    }

    fn handle_counter(&self, stat_type: StatType, count: i64, label: Option<&str>) {
        let stat_string = stat_type.to_string();
        let sized_count: usize = count.try_into().expect("Could not conert to usize");
        match label {
            Some(label) => counter!(stat_string, MSG_TYPE => format!("{}", label))
                .increment(sized_count.try_into().unwrap()),
            None => counter!(stat_string).increment(sized_count.try_into().unwrap()),
        };
    }
}

/// Stat types for the various metrics reported by the application
#[derive(Clone, Debug, Display, Hash, PartialEq, Eq)]
pub(crate) enum StatType {
    //
    // counters
    //
    /// Counter for a deserialized message.
    #[strum(serialize = "messages.skipped")]
    MessageSkipped,
    /// Counter for a deserialized message.
    #[strum(serialize = "messages.deserialization.completed")]
    MessageDeserialized,
    /// Counter for a message that failed deserialization.
    #[strum(serialize = "messages.deserialization.failed")]
    MessageDeserializationFailed,
    /// Counter for a message that failed transformation.
    #[strum(serialize = "messages.transform.failed")]
    MessageTransformFailed,
    /// Counter for when a record batch is started.
    #[strum(serialize = "recordbatch.started")]
    RecordBatchStarted,
    /// Counter for when a record batch is completed.
    #[strum(serialize = "recordbatch.completed")]
    RecordBatchCompleted,
    /// Counter for Size of Write.
    #[strum(serialize = "delta.write.size")]
    DeltaWriteSize,
    /// Counter for when a delta write is started.
    #[strum(serialize = "delta.write.started")]
    DeltaWriteStarted,
    /// Counter for when a delta write is completed.
    #[strum(serialize = "delta.write.completed")]
    DeltaWriteCompleted,
    /// Counter for failed delta writes.
    #[strum(serialize = "delta.write.failed")]
    DeltaWriteFailed,

    //
    // timers
    //
    /// Timer for record batch write duration.
    #[strum(serialize = "recordbatch.write_duration")]
    RecordBatchWriteDuration,
    /// Timer for delta write duration.
    #[strum(serialize = "delta.write.duration")]
    DeltaWriteDuration,
    /// Timer for transform duration.
    #[strum(serialize = "transform.duration")]
    TransformDuration,
    /// Timer for deserialize duration.
    #[strum(serialize = "deserialize.duration")]
    MessageDeserializedDuration,

    //
    // gauges
    //
    /// Gauge for number of Arrow record batches in buffer.
    #[strum(serialize = "buffered.record_batches")]
    BufferedRecordBatches,
    /// Gauge for the number of partitions in the last delta write.
    #[strum(serialize = "delta.write.lag.num_partitions")]
    DeltaWriteNumPartitions,
}

/// Creates a statsd metric scope to send metrics to.
fn create_queue(endpoint: Option<String>) -> Result<InputQueueScope, IngestMetricsError> {
    let input_queue_size = if let Ok(val) = std::env::var(METRICS_INPUT_QUEUE_SIZE_VAR_NAME) {
        val.parse::<usize>()
            .map_err(|_| IngestMetricsError::InvalidMetricsInputQueueSize(val))?
    } else {
        DEFAULT_INPUT_QUEUE_SIZE
    };

    let prefix =
        std::env::var(METRICS_PREFIX_VAR_NAME).unwrap_or_else(|_| "kafka_delta_ingest".to_string());
    let endpoint = match endpoint {
        Some(endpoint) => endpoint.to_string(),
        None => {
            let s = statsd_mock::start();
            let addr = s.addr();
            Box::leak(Box::new(s));
            addr
        }
    };
    let scope = Statsd::send_to(endpoint)
        .unwrap()
        // don't send stats immediately -
        // wait to trigger on input queue size
        .queued(input_queue_size)
        .named(prefix)
        .metrics();

    Ok(scope)
}
