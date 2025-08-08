use std::time::Duration;

use anyhow::Context;
use tokio::time::Instant;

use crate::{MapValue, MapValueContent, State};

pub mod list;

pub async fn set(state: &State, args: &[String]) -> anyhow::Result<Option<serde_json::Value>> {
    let [key, value, ..] = args else {
        todo!("args.len() < 2");
    };

    let value = MapValue {
        value: MapValueContent::String(value.clone()),
        expires_at: if args.len() > 2 && args[2].eq_ignore_ascii_case("px") {
            let ms: u64 = args[3].parse().context("parsing millis until expiration")?;

            Some(Instant::now() + Duration::from_millis(ms))
        } else {
            None
        },
    };

    state.map.insert(key.clone(), value);
    Ok(Some(serde_json::json!("OK")))
}

pub async fn get(state: &State, args: &[String]) -> anyhow::Result<Option<serde_json::Value>> {
    let key = &args[0];
    let value = if let Some(value) = state.map.get(key) {
        if value.expires_at.is_none_or(|e| Instant::now() < e) {
            eprintln!("get {key} from map -> {:?}", value.value);
            match &value.value {
                MapValueContent::String(string) => serde_json::Value::String(string.clone()),
                MapValueContent::List(_) => serde_json::Value::Null,
            }
        } else {
            drop(value);
            state.map.remove(key);
            eprintln!("remove {key} from map because expired");
            serde_json::Value::Null
        }
    } else {
        eprintln!("get {key} from map -> (nil)");
        serde_json::Value::Null
    };

    Ok(Some(value))
}
