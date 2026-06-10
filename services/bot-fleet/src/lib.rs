//! This module implements lib behavior.
//!
//! It belongs to the IICPC benchmarking platform and should keep its
//! behavior consistent with the service contracts documented in design.md.
//! The comments in this file describe public structure and callable behavior.
pub mod config;
pub mod content;
pub mod errors;
pub mod fix;
pub mod kafka;
pub mod metrics;
pub mod telemetry;
pub mod time;
pub mod worker;
