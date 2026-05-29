use thiserror::Error;

#[derive(Error, Debug)]
pub enum BotFleetError {
    // KafkaError was used by the kafka-rust path to surface poisoned-mutex
    // failures from spawn_blocking. After the rdkafka migration the
    // FutureProducer and StreamConsumer are Send+Sync directly, so no mutex
    // wrapping and no synthetic error variant. Kafka failures bubble up as
    // anyhow errors with the librdkafka message intact.
    #[error("Telemetry error: {0}")]
    TelemetryError(String),
    #[error("Validation error: {0}")]
    ValidationError(String),
}
