use std::time::{SystemTime, UNIX_EPOCH};

/// unix_nanos returns the current realtime clock as nanoseconds since Unix epoch.
/// It is used for cross-process telemetry and controller barrier timestamps.
pub fn unix_nanos() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos() as u64
}
