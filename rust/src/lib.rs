use std::time::{SystemTime, UNIX_EPOCH};

pub mod proto {
    include!(concat!(env!("OUT_DIR"), "/_.rs"));
}

pub fn agora_ms() -> f64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs_f64()
        * 1000.0
}
