//! This module implements errors behavior.
//!
//! It belongs to the IICPC benchmarking platform and should keep its
//! behavior consistent with the service contracts documented in design.md.
//! The comments in this file describe public structure and callable behavior.

use thiserror::Error;

#[derive(Error, Debug)]
/// BotFleetError enumerates the states or variants handled by this module.
/// Match arms should preserve the semantic contract of each variant.
pub enum BotFleetError {
    #[error("Telemetry error: {0}")]
    TelemetryError(String),
    #[error("Validation error: {0}")]
    ValidationError(String),
}
