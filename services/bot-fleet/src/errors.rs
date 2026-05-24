use thiserror::Error;

#[derive(Error, Debug)]
pub enum BotFleetError {
    #[error("Kafka error: {0}")]
    KafkaError(String),
    #[error("Telemetry error: {0}")]
    TelemetryError(String),
    #[error("Validation error: {0}")]
    ValidationError(String),
}

