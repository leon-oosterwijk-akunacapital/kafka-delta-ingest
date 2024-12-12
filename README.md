# Notes on this fork
This fork is different from its original in some important ways:
* Removed Azure support -- Not needed
* Removed dynamic schema evolution -- Would like to add back at some point
* Added support for a metrics endpoint so it can run in K8S with Prometheus Service Monitors
* Made parallel: deserialize, transform, convert to arrow, DLQ management
* Support for multiple message types on a single kafka topic
* Support for automatically creating the tables for new message types
* Support for white listing message types to sink
* Support for Protobuf as a message type -- although some Akuna specific functions are missing from this repo
* Added a table repair function to handle S3 bucket lifecycle policy purging old data

# kafka-delta-ingest

The kafka-delta-ingest project aims to build a highly efficient daemon for
streaming data through link:https://kafka.apache.org[Apache Kafka] into
link:https://delta.io[Delta Lake].

This project is currently in production in a number of organizations and is
still actively evolving in tandem with the
link:https://github.com/delta-io/delta-rs[delta-rs] bindings.

To contribute please look at the link:https://github.com/delta-io/kafka-delta-ingest/blob/main/doc/HACKING.adoc[hacking document].

## Features

* Multiple worker processes per stream
* Basic transformations within message
* Statsd or Grafana metric output
* Creates tables if no tables are present in the target bucket
* Supports multiple message types per topic.
* Supports JSON, Avro, Protobuf (Schema Registry), Akuna Protocol (AIDL)
* Configurable threadpool sizes for serialisation, transformation, and writing.
* Repair and optimize mode to optimize Delta tables

See the doc/DESIGN.md for more details.

### Example

The repository includes an example for trying out the application locally with some fake web request data.

The included docker-compose.yml contains kafka and link:https://github.com/localstack/localstack[localstack] services you can run `kafka-delta-ingest` against locally.

Note that the example does not currently work against the localstack due to some incompatibility between more recent libraries and localstack. See the other .sh files under bin/ for examples against a real Kafka cluster.

#### Starting Worker Processes

1. Launch test services - `docker-compose up setup`
2. Compile: `cargo build`
3. Run kafka-delta-ingest against the web_requests example topic and table (customize arguments as desired)

See the start commands under bin for some examples.

Notes:

* The AWS_* environment variables are for S3 and are required by the delta-rs library.
** Above, AWS_ENDPOINT_URL points to localstack.
* The Kafka broker is assumed to be at localhost:9092, use -k to override.
* To clean data from previous local runs, execute `./bin/clean-example-data.sh`. You'll need to do this if you destroy your Kafka container between runs since your delta log directory will be out of sync with Kafka offsets.

#### Kafka SSL

In case you have Kafka topics secured by SSL client certificates, you can specify these secrets as environment variables.

For the cert chain include the PEM content as an environment variable named `KAFKA_DELTA_INGEST_CERT`.
For the cert private key include the PEM content as an environment variable named `KAFKA_DELTA_INGEST_KEY`.

These will be set as the `ssl.certificate.pem` and `ssl.key.pem` Kafka settings respectively.

Make sure to provide the additional option:

```
-K security.protocol=SSL
```

when invoking the cli command as well.

## Build the docker image

```
docker build
```

Notes:

* If this takes a long time, make sure that docker has enough memory

## Writing to S3

When writing to S3, you may experience an error like `source: StorageError { source: S3Generic("dynamodb locking is not enabled") }`.

A locking mechanism is need to prevent unsafe concurrent writes to a delta lake directory, and DynamoDB is an option for this. To use DynamoDB, set the `AWS_S3_LOCKING_PROVIDER` variable to `dynamodb` and create a table named `delta_rs_lock_table` in Dynamo. An example DynamoDB table creation snippet using the aws CLI follows, and should be customized for your environment's needs (e.g. read/write capacity modes):


```bash
aws dynamodb create-table --table-name delta_rs_lock_table \
    --attribute-definitions \
        AttributeName=key,AttributeType=S \
    --key-schema \
        AttributeName=key,KeyType=HASH \
    --provisioned-throughput \
        ReadCapacityUnits=10,WriteCapacityUnits=10
```

## Schema Support
This application has support for avro,json, Schema Registry Protobuf, and Akuna AIDL (protocol) format via arguments. If no format argument is provided, the default behavior is to use json.
The table below indicates what will happen with respect to the provided arguments.


| Argument      | Value | Result                                                                      |
| ----------- | ----------- |-----------------------------------------------------------------------------|
| none      | none       | default json behavior                                                       |
| --json      | any string       | default json behavior                                                       |
| --json      | schema registry url       | will connect schema registry to deserialize json                            |
| --avro   | ""        | expects all messages in avro format                                         |
| --avro      | path to an avro schema     | will use the provided avro schema for deserialization                       |
| --avro   | schema registry url        | will connect schema registry to deserialize avro                            |
| --protobuf | schema registry url        | will connect schema registry to deserialize protobuf (Akuna Only)           |
| --aidl | schema registry url        | will connect to protocol schema registry to deserialize msgpack (protocol)  (Akuna Only) |



For more information, see link:https://github.com/delta-io/delta-rs/tree/dbc2994c5fddfd39fc31a8f9202df74788f59a01/dynamodb_lock[DynamoDB lock].

### Argument Reference

```
kafka-delta-ingest help repair
Repair a Delta table

Usage: kafka-delta-ingest repair [OPTIONS] --table_url <table_url>

Options:
      --table_url <table_url>
          The URL under which the table exists. [env: TABLE_URL=]
      --optimize_target_size <optimize_target_size>
          The target size for the optimized files [env: OPTIMIZE_TARGET_SIZE=] [default: 100000000]
      --optimize_partition <optimize_partition>
          The partition to optimize [env: OPTIMIZE_PARTITION=]
      --optimize_partition_predicate <optimize_partition_predicate>
          The predicate for the partition to optimize [env: OPTIMIZE_PARTITION_PREDICATE=]
  -h, --help
          Print help
          
kafka-delta-ingest help ingest
Starts a stream that consumes from a Kafka topic and writes to a Delta table

Usage: kafka-delta-ingest ingest [OPTIONS] --table_base_uri <table_base_uri> --topic <topic>

Options:
      --table_base_uri <table_base_uri>
          The Base URI under which all tables will be created/written to. [env: TABLE_BASE_URI=]
      --message_type_whitelist <message_type_whitelist>
          A Regex which will be used to filter messages based on their type. Only messages that match the regex will be ingested. If this field is not provided all message types will be saved. [env: MESSAGE_TYPE_WHITELIST=]
      --topic <topic>
          The Kafka topic to stream from [env: KAFKA_TOPIC=]
  -k, --kafka <kafka>
          Kafka broker connection string to use [env: KAFKA_BROKERS=] [default: localhost:9092]
  -g, --consumer_group <consumer_group>
          Consumer group to use when subscribing to Kafka topics [env: KAFKA_CONSUMER_GROUP=] [default: kafka_delta_ingest]
  -a, --app_id <app_id>
          App ID to use when writing to Delta [env: APP_ID=] [default: kafka_delta_ingest]
      --seek_offsets <seek_offsets>
          Only useful when offsets are not already stored in the delta table. A JSON string specifying the partition offset map as the starting point for ingestion. This is *seeking* rather than _starting_ offsets. The first ingested message would be (seek_offset + 1). Ex: {"0":123, "1":321} [env: KAFKA_SEEK_OFFSETS=]
  -o, --auto_offset_reset <auto_offset_reset>
          The default offset reset policy, which is either 'earliest' or 'latest'.
          The configuration is applied when offsets are not found in delta table or not specified with 'seek_offsets'. This also overrides the kafka consumer's 'auto.offset.reset' config. [env: KAFKA_AUTO_OFFSET_RESET=] [default: earliest]
  -l, --allowed_latency <allowed_latency>
          The allowed latency (in seconds) from the time a message is consumed to when it should be written to Delta. [env: ALLOWED_LATENCY=] [default: 300]
  -m, --max_messages_per_batch <max_messages_per_batch>
          The maximum number of rows allowed in a parquet batch. This shoulid be the approximate number of bytes described by MIN_BYTES_PER_FILE [env: MAX_MESSAGES_PER_BATCH=] [default: 1000000]
      --housekeeping_loop_iteration <housekeeping_loop_iteration>
          The number of message receive loops to execute before performing housekeeping tasks. [env: HOUSEKEEPING_LOOP_ITERATION=] [default: 5000]
      --deserialization_workers <deserialization_workers>
          The number of workers to spawn to process deserializing messages [env: DESERIALIZATION_WORKERS=] [default: 10]
      --num_arrow_writer_threads <num_arrow_writer_threads>
          The number of threads to spawn to process converting JSON into arrow recordbatches [env: NUM_ARROW_WRITER_THREADS=] [default: 4]
  -b, --min_bytes_per_file <min_bytes_per_file>
          The target minimum file size (in bytes) for each Delta file. File size may be smaller than this value if ALLOWED_LATENCY does not allow enough time to accumulate the specified number of bytes. [env: MIN_BYTES_PER_FILE=] [default: 134217728]
      --partition_columns <partition_columns>
          When creating a new delta table, use the provided list of columns for the partitioning. The list if there is more than one should be comma seperated. [env: PARTITION_COLUMNS=]
  -t, --transform <transform>
          A list of transforms to apply to each Kafka message. Each transform should follow the pattern:
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
           [env: TRANSFORMS=]
      --dlq_table_location <dlq_table_location>
          Optional table to write unprocessable entities to [env: DLQ_TABLE_LOCATION=]
      --dlq_transform <dlq_transform>
          Transforms to apply before writing unprocessable entities to the dlq_location [env: DLQ_TRANSFORMS=]
  -c, --checkpoints
          If set then kafka-delta-ingest will write checkpoints on every 10th commit [env: WRITE_CHECKPOINTS=]
  -K, --kafka_setting <kafka_setting>
          A list of additional settings to include when creating the Kafka consumer.
          
          This can be used to provide TLS configuration as in:
          
          ... -K "security.protocol=SSL" "ssl.certificate.location=kafka.crt" "ssl.key.location=kafka.key"
  -s, --statsd_endpoint <statsd_endpoint>
          Statsd endpoint for sending stats [env: STATSD_ENDPOINT=]
      --seconds_idle_kafka_read_before_flush <seconds_idle_kafka_read_before_flush>
          The number of seconds to wait before flushing the table state when no messages are being read. [env: SECONDS_IDLE_KAFKA_READ_BEFORE_FLUSH=] [default: 180]
      --worker_to_messages_multiplier <worker_to_messages_multiplier>
          The multiplier for how many messages to send to each deserialisation worker per loop iteration. [env: WORKER_TO_MESSAGES_MULTIPLIER=] [default: 10000]
      --num_converting_workers <num_converting_workers>
          The Number of Converting workers per type to create. These workers will apply the transform and coerce. The number of workers should be balanced against the deserialisation workers to achieve max throughput. [env: NUM_CONVERTING_WORKERS=] [default: 4]
      --prometheus_port <prometheus_port>
          Port where prometheus stats will be exposed [env: PROMETHEUS_PORT=]
      --json <json>
          Schema registry endpoint, local path, or empty string [env: JSON_REGISTRY=]
      --protobuf <protobuf>
          Schema registry endpoint [env: PROTOBUF_REGISTRY=]
      --avro <avro>
          Schema registry endpoint, local path, or empty string [env: AVRO_REGISTRY=]
      --aidl <aidl>
          AIDL rest registry endpoint, local path, or empty string [env: AIDL_REGISTRY=]
  -e, --ends_at_latest_offsets
          [env: TURN_OFF_AT_KAFKA_TOPIC_END=]
  -h, --help
          Print help

```
