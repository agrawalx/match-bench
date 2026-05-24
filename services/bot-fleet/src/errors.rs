use std::fmt::Display;

use thiserror::Error;

#[derive(Error, Debug)]
pub enum BotFleetError {
    ConfigError(String),
    KafkaError(String),
    SerializationError(String),
    ConnectionError(String),
    TelemetryError(String),
    WorkerError(String),
    ValidationError(String),
}

impl Display for BotFleetError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{self:?}")
    }
}
