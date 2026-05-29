use std::time::{SystemTime, UNIX_EPOCH};

/// unix_nanos returns the current realtime clock as nanoseconds since Unix epoch.
/// It is used for cross-process telemetry and controller barrier timestamps.
pub fn unix_nanos() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system clock is before Unix epoch; realtime timestamps are invalid")
        .as_nanos() as u64
}

/// format_fix_timestamp formats a UNIX nanosecond timestamp into the standard
/// FIX SendingTime format: `YYYYMMDD-HH:MM:SS.mmm` in UTC.
///
/// Bypasses Chrono to perform zero-allocation stack formatting.
pub fn format_fix_timestamp(nanos: u64) -> [u8; crate::fix::FIX_TIMESTAMP_LEN] {
    let millis = nanos / 1_000_000;
    let total_secs = millis / 1000;
    let ms = millis % 1000;

    let total_mins = total_secs / 60;
    let sec = total_secs % 60;

    let total_hours = total_mins / 60;
    let min = total_mins % 60;

    let total_days = total_hours / 24;
    let hour = total_hours % 24;

    // Epoch calendar calculation (1970-01-01 was Thursday)
    let mut year = 1970;
    let mut days_left = total_days;

    loop {
        let is_leap = (year % 4 == 0 && year % 100 != 0) || (year % 400 == 0);
        let days_in_year = if is_leap { 366 } else { 365 };
        if days_left < days_in_year {
            break;
        }
        days_left -= days_in_year;
        year += 1;
    }

    let is_leap = (year % 4 == 0 && year % 100 != 0) || (year % 400 == 0);
    let month_days = if is_leap {
        [31, 29, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31]
    } else {
        [31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31]
    };

    let mut month = 1;
    for &days in &month_days {
        if days_left < days {
            break;
        }
        days_left -= days;
        month += 1;
    }
    let day = days_left + 1;

    let mut buf = [0u8; crate::fix::FIX_TIMESTAMP_LEN];
    // YYYYMMDD-HH:MM:SS.mmm
    buf[0] = b'0' + (year / 1000) as u8;
    buf[1] = b'0' + ((year / 100) % 10) as u8;
    buf[2] = b'0' + ((year / 10) % 10) as u8;
    buf[3] = b'0' + (year % 10) as u8;

    buf[4] = b'0' + (month / 10) as u8;
    buf[5] = b'0' + (month % 10) as u8;

    buf[6] = b'0' + (day / 10) as u8;
    buf[7] = b'0' + (day % 10) as u8;

    buf[8] = b'-';

    buf[9] = b'0' + (hour / 10) as u8;
    buf[10] = b'0' + (hour % 10) as u8;

    buf[11] = b':';

    buf[12] = b'0' + (min / 10) as u8;
    buf[13] = b'0' + (min % 10) as u8;

    buf[14] = b':';

    buf[15] = b'0' + (sec / 10) as u8;
    buf[16] = b'0' + (sec % 10) as u8;

    buf[17] = b'.';

    buf[18] = b'0' + (ms / 100) as u8;
    buf[19] = b'0' + ((ms / 10) % 10) as u8;
    buf[20] = b'0' + (ms % 10) as u8;

    buf
}
