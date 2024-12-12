use std::fmt::{Display, Formatter};
use std::{convert::TryFrom, io::Cursor, path::PathBuf};

use async_trait::async_trait;

use schema_registry_converter::async_impl::{
    easy_avro::EasyAvroDecoder, easy_json::EasyJsonDecoder, schema_registry::SrSettings,
};

use serde_json::Value;

//use crate::{MAP_MARKER_ENTRY, SolaceRecord};
use crate::{dead_letters::DeadLetter, MessageDeserializationError, MessageFormat};

// cloneable type
#[derive(Clone, Default, Eq, Hash, PartialEq, Debug)]
pub struct MessageType {
    name: std::string::String,
}

impl MessageType {
    pub(crate) fn as_str(&self) -> &str {
        self.name.as_str()
    }
}

impl Display for MessageType {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        self.name.fmt(f)
    }
}

// implement from string for MessageType
impl From<&std::string::String> for MessageType {
    fn from(name: &std::string::String) -> Self {
        MessageType { name: name.clone() }
    }
}

#[async_trait]
pub(crate) trait MessageDeserializer {
    async fn deserialize(
        &self,
        message_bytes: &[u8],
    ) -> Result<(MessageType, Value), MessageDeserializationError>;
}

pub(crate) struct MessageDeserializerFactory {}

impl MessageDeserializerFactory {
    pub fn try_build(
        input_format: &MessageFormat,
        default_message_type: MessageType,
    ) -> Result<Box<dyn MessageDeserializer + Send + Sync>, anyhow::Error> {
        match input_format {
            MessageFormat::Json(data) => match data {
                crate::SchemaSource::None => Ok(Self::json_default(default_message_type)),
                crate::SchemaSource::SchemaRegistry(sr) => {
                    match Self::build_sr_settings(sr).map(JsonDeserializer::from_schema_registry) {
                        Ok(s) => Ok(Box::new(s)),
                        Err(e) => Err(e),
                    }
                }
                crate::SchemaSource::File(_) => Ok(Self::json_default(default_message_type)),
            },
            MessageFormat::Avro(data) => match data {
                crate::SchemaSource::None => Ok(Box::<AvroSchemaDeserializer>::default()),
                crate::SchemaSource::SchemaRegistry(sr) => {
                    match Self::build_sr_settings(sr)
                        .map(|s| AvroDeserializer::from_schema_registry(s, default_message_type))
                    {
                        Ok(s) => Ok(Box::new(s)),
                        Err(e) => Err(e),
                    }
                }
                crate::SchemaSource::File(f) => {
                    match AvroSchemaDeserializer::try_from_schema_file(f, default_message_type) {
                        Ok(s) => Ok(Box::new(s)),
                        Err(e) => Err(e),
                    }
                }
            },
            // enabled only if akunaformats is enabled
            #[cfg(feature = "akunaformats")]
            MessageFormat::Protobuf(schema_source) => match schema_source {
                crate::SchemaSource::None => Err(anyhow::Error::msg("Unimplemented")),
                crate::SchemaSource::SchemaRegistry(sr) => {
                    match Self::build_sr_settings(sr)
                        .map(akuna::ProtobufDeserializer::from_schema_registry)
                    {
                        Ok(s) => Ok(Box::new(s)),
                        Err(e) => Err(e),
                    }
                }
                crate::SchemaSource::File(_f) => Err(anyhow::Error::msg("Unimplemented")),
            },
            #[cfg(feature = "akunaformats")]
            MessageFormat::Aidl(schema_source) => match schema_source {
                crate::SchemaSource::None => Err(anyhow::Error::msg("Unimplemented")),
                crate::SchemaSource::SchemaRegistry(sr) => Ok(Box::new(
                    akuna::AidlDeserializer::from_aidl_registry(sr.to_string()),
                )),
                crate::SchemaSource::File(_f) => Err(anyhow::Error::msg("Unimplemented")),
            },
            _ => Ok(Box::new(DefaultDeserializer {
                default_message_type,
            })),
        }
    }

    pub(crate) fn json_default(
        default_message_type: MessageType,
    ) -> Box<dyn MessageDeserializer + Send + Sync> {
        Box::new(DefaultDeserializer {
            default_message_type,
        })
    }

    fn build_sr_settings(registry_url: &url::Url) -> Result<SrSettings, anyhow::Error> {
        let mut url_string = registry_url.as_str();
        if url_string.ends_with('/') {
            url_string = &url_string[0..url_string.len() - 1];
        }

        let mut builder = SrSettings::new_builder(url_string.to_owned());
        if let Ok(username) = std::env::var("SCHEMA_REGISTRY_USERNAME") {
            builder.set_basic_authorization(
                username.as_str(),
                option_env!("SCHEMA_REGISTRY_PASSWORD"),
            );
        }

        if let Ok(proxy_url) = std::env::var("SCHEMA_REGISTRY_PROXY") {
            builder.set_proxy(proxy_url.as_str());
        }

        match builder.build() {
            Ok(s) => Ok(s),
            Err(e) => Err(anyhow::Error::new(e)),
        }
    }
}

struct DefaultDeserializer {
    default_message_type: MessageType,
}

#[async_trait]
impl MessageDeserializer for DefaultDeserializer {
    async fn deserialize(
        &self,
        payload: &[u8],
    ) -> Result<(MessageType, Value), MessageDeserializationError> {
        let value: Value = match serde_json::from_slice(payload) {
            Ok(v) => v,
            Err(e) => {
                return Err(MessageDeserializationError::JsonDeserialization {
                    dead_letter: DeadLetter::from_failed_deserialization(payload, e.to_string()),
                });
            }
        };

        Ok((self.default_message_type.clone(), value))
    }
}

struct AvroDeserializer {
    default_message_type: MessageType,
    decoder: EasyAvroDecoder,
}

#[derive(Default)]
struct AvroSchemaDeserializer {
    default_message_type: MessageType,
    schema: Option<apache_avro::Schema>,
}

struct JsonDeserializer {
    decoder: EasyJsonDecoder,
}

#[async_trait]
impl MessageDeserializer for AvroDeserializer {
    async fn deserialize(
        &self,
        message_bytes: &[u8],
    ) -> Result<(MessageType, Value), MessageDeserializationError> {
        match self.decoder.decode_with_schema(Some(message_bytes)).await {
            Ok(drs) => match drs {
                Some(v) => {
                    let t = match v.name {
                        Some(name) => MessageType { name: name.name },
                        None => self.default_message_type.clone(),
                    };
                    match Value::try_from(v.value) {
                        Ok(v) => Ok((t, v)),
                        Err(e) => Err(MessageDeserializationError::AvroDeserialization {
                            dead_letter: DeadLetter::from_failed_deserialization(
                                message_bytes,
                                e.to_string(),
                            ),
                        }),
                    }
                }
                None => return Err(MessageDeserializationError::EmptyPayload),
            },
            Err(e) => {
                return Err(MessageDeserializationError::AvroDeserialization {
                    dead_letter: DeadLetter::from_failed_deserialization(
                        message_bytes,
                        e.to_string(),
                    ),
                });
            }
        }
    }
}

#[async_trait]
impl MessageDeserializer for AvroSchemaDeserializer {
    async fn deserialize(
        &self,
        message_bytes: &[u8],
    ) -> Result<(MessageType, Value), MessageDeserializationError> {
        let reader_result = match &self.schema {
            None => apache_avro::Reader::new(Cursor::new(message_bytes)),
            Some(schema) => apache_avro::Reader::with_schema(schema, Cursor::new(message_bytes)),
        };

        match reader_result {
            Ok(mut reader) => {
                if let Some(r) = reader.next() {
                    let v = match r {
                        Err(_) => return Err(MessageDeserializationError::EmptyPayload),
                        Ok(v) => Value::try_from(v),
                    };

                    return match v {
                        Ok(value) => Ok((self.default_message_type.clone(), value)), // not sure how to get the schema type from this reader.
                        Err(e) => Err(MessageDeserializationError::AvroDeserialization {
                            dead_letter: DeadLetter::from_failed_deserialization(
                                message_bytes,
                                e.to_string(),
                            ),
                        }),
                    };
                }

                return Err(MessageDeserializationError::EmptyPayload);
                // TODO: Code to return multiple values from avro message
                /*let (values, errors): (Vec<_>, Vec<_>) =
                    reader.into_iter().partition(Result::is_ok);
                if errors.len() > 0 {
                    let error_string = errors
                        .iter()
                        .map(|m| m.err().unwrap().to_string())
                        .fold(String::new(), |current, next| current + "\n" + &next);
                    return Err(MessageDeserializationError::AvroDeserialization {
                        dead_letter: DeadLetter::from_failed_deserialization(
                            message_bytes,
                            error_string,
                        ),
                    });
                }
                let (transformed, t_errors): (Vec<_>, Vec<_>) = values
                    .into_iter()
                    .map(|v| v.unwrap())
                    .map(Value::try_from)
                    .partition(Result::is_ok);

                if t_errors.len() > 0 {
                    let error_string = t_errors
                        .iter()
                        .map(|m| m.err().unwrap().to_string())
                        .fold(String::new(), |current, next| current + "\n" + &next);
                    return Err(MessageDeserializationError::AvroDeserialization {
                        dead_letter: DeadLetter::from_failed_deserialization(
                            message_bytes,
                            error_string,
                        ),
                    });
                }

                Ok(transformed.into_iter().map(|m| m.unwrap()).collect())*/
            }
            Err(e) => Err(MessageDeserializationError::AvroDeserialization {
                dead_letter: DeadLetter::from_failed_deserialization(message_bytes, e.to_string()),
            }),
        }
    }
}

#[async_trait]
impl MessageDeserializer for JsonDeserializer {
    async fn deserialize(
        &self,
        message_bytes: &[u8],
    ) -> Result<(MessageType, Value), MessageDeserializationError> {
        let decoder = &self.decoder;
        match decoder.decode(Some(message_bytes)).await {
            Ok(drs) => match drs {
                Some(v) => Ok((
                    MessageType {
                        name: v.schema.url.to_string(),
                    },
                    v.value,
                )),
                None => return Err(MessageDeserializationError::EmptyPayload),
            },
            Err(e) => {
                return Err(MessageDeserializationError::AvroDeserialization {
                    dead_letter: DeadLetter::from_failed_deserialization(
                        message_bytes,
                        e.to_string(),
                    ),
                });
            }
        }
    }
}

impl JsonDeserializer {
    pub(crate) fn from_schema_registry(sr_settings: SrSettings) -> Self {
        JsonDeserializer {
            decoder: EasyJsonDecoder::new(sr_settings.clone()),
        }
    }
}

impl AvroSchemaDeserializer {
    pub(crate) fn try_from_schema_file(
        file: &PathBuf,
        default_message_type: MessageType,
    ) -> Result<Self, anyhow::Error> {
        match std::fs::read_to_string(file) {
            Ok(content) => match apache_avro::Schema::parse_str(&content) {
                Ok(s) => Ok(AvroSchemaDeserializer {
                    default_message_type,
                    schema: Some(s),
                }),
                Err(e) => Err(anyhow::format_err!("{}", e.to_string())),
            },
            Err(e) => Err(anyhow::format_err!("{}", e.to_string())),
        }
    }
}

impl AvroDeserializer {
    pub(crate) fn from_schema_registry(
        sr_settings: SrSettings,
        default_message_type: MessageType,
    ) -> Self {
        AvroDeserializer {
            default_message_type,
            decoder: EasyAvroDecoder::new(sr_settings.clone()),
        }
    }
}

#[cfg(feature = "akunaformats")]
mod akuna {
    const SOLACE_PREFIX: &str = "solace_";
    const EMPTY_STRING: &str = "";

    //use crate::serialization::akuna::SOLACE_PREFIX;
    use akuna_deserializer::protobuf_decoder::{convert_epoch_timestamp, convert_fixed_point};
    use akuna_deserializer::protocol_decoder::{AidlDecoder, ProtocolDecoder, MAP_MARKER_ENTRY};
    use akuna_deserializer::SolaceRecord;
    use async_trait::async_trait;
    use log::{error, trace};
    use protobuf::Message as ProtobufMessage;
    use protofish::context::MessageRef;
    use protofish::decode::PackedArray;
    use protofish::decode::Value::{
        Bool, Bytes, Double, Enum, Fixed32, Fixed64, Float, Incomplete, Int32, Int64, Message,
        Packed, SFixed32, SFixed64, SInt32, SInt64, String, UInt32, UInt64, Unknown,
    };
    use schema_registry_converter::async_impl::easy_proto_decoder::EasyProtoDecoder;
    use schema_registry_converter::async_impl::proto_decoder::{
        DecodeContext, DecodeResultWithContext,
    };
    use schema_registry_converter::async_impl::schema_registry::SrSettings;
    use serde_json::Value;
    use crate::dead_letters::DeadLetter;
    use crate::MessageDeserializationError;
    use crate::serialization::{MessageDeserializer, MessageType};

    pub(crate) struct ProtobufDeserializer {
        decoder: EasyProtoDecoder,
        converter: ProtobufToJsonValueConverter,
    }

    #[async_trait]
    impl MessageDeserializer for AidlDeserializer {
        async fn deserialize(
            &self,
            message_bytes: &[u8],
        ) -> Result<(MessageType, Value), MessageDeserializationError> {
            match SolaceRecord::SolaceRecord::parse_from_bytes(message_bytes) {
                Ok(record) => {
                    let crc = AidlDecoder::get_crc(record.user_data.as_slice());

                    let name = match self.decoder.get_protocol_name_by_crc(crc).await {
                        Ok(name) => name,
                        Err(e) => {
                            error!("Error getting protocol name by CRC: {:?}", e);
                            return Err(MessageDeserializationError::AIDLDeserialization {
                                dead_letter: DeadLetter::from_failed_deserialization(
                                    message_bytes,
                                    e.to_string(),
                                ),
                            });
                        }
                    };

                    let mut decoded_value = match self
                        .decoder
                        .decode_by_crc(crc, record.binary_attachment.as_slice())
                        .await
                    {
                        Ok(value) => Value::Object(
                            flatten(EMPTY_STRING.to_string(), value)?
                                .into_iter()
                                .collect(),
                        ),
                        Err(e) => {
                            trace!("Error decoding by CRC: {:?}", e);
                            return Err(MessageDeserializationError::AIDLDeserialization {
                                dead_letter: DeadLetter::from_failed_deserialization(
                                    message_bytes,
                                    e.to_string(),
                                ),
                            });
                        }
                    };

                    let value_map = decoded_value.as_object_mut().unwrap();

                    value_map.insert(
                        format!("{}topic", SOLACE_PREFIX),
                        Value::String(record.topic.name.clone()),
                    );
                    value_map.insert(
                        format!("{}sender_id", SOLACE_PREFIX),
                        Value::String(record.sender_id.clone()),
                    );
                    value_map.insert(
                        format!("{}sender_ts_millis", SOLACE_PREFIX),
                        Value::Number(record.sender_ts_millis.into()),
                    );
                    value_map.insert(
                        format!("{}seq_num", SOLACE_PREFIX),
                        Value::Number(record.seq_num.into()),
                    );
                    let value = Value::Object(value_map.to_owned());

                    Ok((MessageType { name }, value))
                }
                Err(e) => {
                    error!("Error decoding protobuf message: {:?}", e);
                    Err(MessageDeserializationError::AkunaProtobufDeserialization {
                        dead_letter: DeadLetter::from_failed_deserialization(
                            message_bytes,
                            e.to_string(),
                        ),
                    })
                }
            }
        }
    }

    #[async_trait]
    impl MessageDeserializer for ProtobufDeserializer {
        async fn deserialize(
            &self,
            message_bytes: &[u8],
        ) -> Result<(MessageType, Value), MessageDeserializationError> {
            match self.decoder.decode_with_context(Some(message_bytes)).await {
                Ok(odwc) => match odwc {
                    Some(v) => {
                        let t = MessageType {
                            name: v.full_name.to_string(),
                        };
                        match self.converter.convert_and_flatten(v) {
                            Ok(v) => Ok((t, v)),
                            Err(e) => {
                                error!("Error converting protobuf message to JSON: {:?}", e);
                                Err(MessageDeserializationError::ProtobufDeserialization {
                                    dead_letter: DeadLetter::from_failed_deserialization(
                                        message_bytes,
                                        e.to_string(),
                                    ),
                                })
                            }
                        }
                    }
                    None => return Err(MessageDeserializationError::EmptyPayload),
                },
                Err(e) => {
                    error!("Error decoding protobuf message: {:?}", e);
                    Err(MessageDeserializationError::ProtobufDeserialization {
                        dead_letter: DeadLetter::from_failed_deserialization(
                            message_bytes,
                            e.to_string(),
                        ),
                    })
                }
            }
        }
    }

    impl ProtobufDeserializer {
        pub(crate) fn from_schema_registry(sr_settings: SrSettings) -> Self {
            //Decoder::new(SrSettings::new(String::from("http://localhost:8081")))
            ProtobufDeserializer {
                decoder: EasyProtoDecoder::new(sr_settings.clone()),
                converter: ProtobufToJsonValueConverter::new(),
            }
        }
    }

    pub(crate) struct AidlDeserializer {
        decoder: AidlDecoder,
    }

    impl AidlDeserializer {
        pub(crate) fn from_aidl_registry(url: std::string::String) -> Self {
            AidlDeserializer {
                decoder: AidlDecoder::new(url),
            }
        }
    }

    struct ProtobufToJsonValueConverter {}

    impl ProtobufToJsonValueConverter {
        pub(crate) fn new() -> Self {
            ProtobufToJsonValueConverter {}
        }

        pub(crate) fn convert(
            &self,
            value: DecodeResultWithContext,
        ) -> Result<Value, MessageDeserializationError> {
            let context = &value.context;

            let mut value_map = serde_json::Map::new();
            let mut deserialize_error: Option<MessageDeserializationError> = None;
            value
                .value
                .fields
                .iter()
                .filter_map(|f| {
                    //log::info!("the field is: {:?}", f);
                    match ProtobufToJsonValueConverter::convert_field(
                        &value.value.msg_ref,
                        &f,
                        context,
                    ) {
                        Ok(t) => Some(t),
                        Err(e) => {
                            deserialize_error = Some(e);
                            None
                        }
                    }
                })
                .for_each(|(k, v)| {
                    if value_map.contains_key(&k) {
                        // replace the key with a vector
                        let existing = value_map.remove(&k).unwrap();
                        match existing {
                            Value::Array(mut array) => {
                                array.push(v);
                                value_map.insert(k, Value::Array(array));
                            }
                            _ => {
                                value_map.insert(k, Value::Array(vec![existing, v]));
                            }
                        }
                    } else {
                        value_map.insert(k, v);
                    }
                });
            match deserialize_error {
                Some(e) => Err(e),
                None => Ok(Value::Object(value_map)),
            }
        }
        pub(crate) fn convert_and_flatten(
            &self,
            value: DecodeResultWithContext,
        ) -> Result<Value, MessageDeserializationError> {
            Ok(Value::Object(
                flatten(EMPTY_STRING.to_string(), self.convert(value)?)?
                    .into_iter()
                    .collect(),
            ))
        }

        fn convert_field(
            msg_ref: &MessageRef,
            f: &protofish::decode::FieldValue,
            context: &DecodeContext,
        ) -> Result<(std::string::String, Value), MessageDeserializationError> {
            let msg_info = context.context.resolve_message(*msg_ref);
            let name = msg_info.get_field(f.number);
            if name.is_none() {
                return Err(MessageDeserializationError::ProtobufDeserialization {
                    dead_letter: DeadLetter::from_failed_deserialization(
                        b"protobuf",
                        format!(
                            "Field number {} not found in message {}",
                            f.number, msg_info.full_name
                        ),
                    ),
                });
            }
            let name = name.unwrap().name.as_str();
            let value = match &f.value {
                Double(f64) => Value::Number(serde_json::Number::from_f64(*f64).unwrap()),
                Float(f32) => Value::Number(serde_json::Number::from_f64(*f32 as f64).unwrap()),
                Int32(i32) => Value::Number(serde_json::Number::from(*i32)),
                Int64(i64) => Value::Number(serde_json::Number::from(*i64)),
                UInt32(u32) => Value::Number(serde_json::Number::from(*u32)),
                UInt64(u64) => Value::Number(serde_json::Number::from(*u64)),
                SInt32(i32) => Value::Number(serde_json::Number::from(*i32)),
                SInt64(i64) => Value::Number(serde_json::Number::from(*i64)),
                Fixed32(u32) => Value::Number(serde_json::Number::from(*u32)),
                Fixed64(u64) => Value::Number(serde_json::Number::from(*u64)),
                SFixed32(i32) => Value::Number(serde_json::Number::from(*i32)),
                SFixed64(i64) => Value::Number(serde_json::Number::from(*i64)),
                Bool(bool) => Value::Bool(*bool),
                String(string) => Value::String(string.to_string()),
                Bytes(bytes) => {
                    Value::String(std::string::String::from_utf8_lossy(&bytes).to_string())
                }
                Packed(packed_array) => {
                    let array = match packed_array {
                        PackedArray::UInt64(array) | PackedArray::Fixed64(array) => array
                            .iter()
                            .map(|i| Value::Number(serde_json::Number::from(*i)))
                            .collect(),
                        PackedArray::Int32(array)
                        | PackedArray::SInt32(array)
                        | PackedArray::SFixed32(array) => array
                            .iter()
                            .map(|i| Value::Number(serde_json::Number::from(*i)))
                            .collect(),
                        PackedArray::Int64(array)
                        | PackedArray::SFixed64(array)
                        | PackedArray::SInt64(array) => array
                            .iter()
                            .map(|i| Value::Number(serde_json::Number::from(*i)))
                            .collect(),
                        PackedArray::Double(array) => array
                            .iter()
                            .map(|i| Value::Number(serde_json::Number::from_f64(*i).unwrap()))
                            .collect(),
                        PackedArray::UInt32(array) | PackedArray::Fixed32(array) => array
                            .iter()
                            .map(|i| Value::Number(serde_json::Number::from(*i)))
                            .collect(),
                        PackedArray::Float(array) => array
                            .iter()
                            .map(|i| {
                                Value::Number(serde_json::Number::from_f64(*i as f64).unwrap())
                            })
                            .collect(),
                        PackedArray::Bool(array) => array.iter().map(|i| Value::Bool(*i)).collect(),
                    };
                    Value::Array(array)
                }
                Message(message_value) => {
                    // custom converters for some Akuna specific message types.
                    // TODO: Put this logic somewhere better.
                    match context
                        .context
                        .resolve_message(message_value.msg_ref)
                        .full_name
                        .as_str()
                    {
                        "akuna.protobuf.numeric.v0.FixedPoint" => {
                            convert_fixed_point(message_value, context)?
                        }
                        "akuna.protobuf.time.v0.EpochTimestamp" => {
                            convert_epoch_timestamp(message_value, context)?
                        }
                        _ => {
                            let map = message_value
                                .fields
                                .iter()
                                .filter_map(|f| {
                                    //log::info!("the field is: {:?}", f);
                                    ProtobufToJsonValueConverter::convert_field(
                                        &message_value.msg_ref,
                                        &f,
                                        &context,
                                    )
                                    .ok()
                                })
                                .collect();
                            Value::Object(map)
                        }
                    }
                }
                Enum(enum_value) => {
                    // use context to convert EnumValue into a String
                    Value::String(
                        context
                            .context
                            .resolve_enum(enum_value.enum_ref)
                            .get_field_by_value(enum_value.value)
                            .map_or_else(|| "MISSING".to_string(), |f| f.name.clone()),
                    )
                }
                Incomplete(_u8, _bytes) => Value::Null,
                Unknown(_unknown_value) => Value::Null,
            };

            Ok((name.to_string(), value))
        }
    }

    /// flatten a tuple of (String, Value) into a Vec of (String, Value) by recursively flattening the Value
    /// prepending the key with the struct name.
    fn flatten(
        struct_name: std::string::String,
        value: Value,
    ) -> Result<Vec<(std::string::String, Value)>, MessageDeserializationError> {
        // convert the Value into a Vec.
        match value {
            Value::Object(m) => {
                // if this object is a map, we should not flatten it.
                if m.contains_key(MAP_MARKER_ENTRY) {
                    return Ok(vec![(struct_name, Value::Object(m))]);
                }
                let struct_name = if struct_name.is_empty() {
                    struct_name
                } else {
                    format!("{}_", struct_name)
                };
                let mut flatten_error: Option<MessageDeserializationError> = None;
                let v: Vec<(std::string::String, Value)> = m
                    .into_iter()
                    .map(|(k, nested_value)| flatten(format!("{}{}", struct_name, k), nested_value))
                    .map_while(|e| match e {
                        Ok(v) => Some(v),
                        Err(e) => {
                            flatten_error = Some(e);
                            None
                        }
                    })
                    .flatten()
                    .collect();
                match flatten_error {
                    Some(e) => Err(e),
                    None => Ok(v),
                }
            }
            _ => Ok(vec![(struct_name, value)]),
        }
    }


}
