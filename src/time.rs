use std::time::{SystemTime, UNIX_EPOCH};

pub fn get_current_timestamp() -> u64 {
    let now = SystemTime::now();
    now.duration_since(UNIX_EPOCH).unwrap().as_millis() as u64
}
