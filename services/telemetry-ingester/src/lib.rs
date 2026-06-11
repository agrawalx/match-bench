//! This module implements lib behavior.
//!
//! It belongs to the IICPC benchmarking platform and should keep its
//! behavior consistent with the service contracts documented in design.md.
//! The comments in this file describe public structure and callable behavior.
pub mod aggregate;
pub mod config;
pub mod ingester;
pub mod join;
pub mod kafka;
pub mod metrics;
pub mod redis_sink;
pub mod rollup;
pub mod store;
