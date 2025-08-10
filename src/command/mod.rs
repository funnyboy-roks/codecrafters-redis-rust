use std::time::Duration;

use anyhow::Context;
use tokio::time::Instant;

use crate::{resp::Value, MapValue, MapValueContent, State};

pub mod list;
pub mod stream;
pub mod transaction;

pub async fn set(state: &State, args: &[String]) -> anyhow::Result<Value> {
    let [key, value, ..] = args else {
        todo!("args.len() < 2");
    };

    let value = MapValue {
        value: MapValueContent::from(&**value),
        expires_at: if args.len() > 2 && args[2].eq_ignore_ascii_case("px") {
            let ms: u64 = args[3].parse().context("parsing millis until expiration")?;

            Some(Instant::now() + Duration::from_millis(ms))
        } else {
            None
        },
    };

    state.map.insert(key.clone(), value);
    Ok(Value::bulk_string("OK"))
}

pub async fn get(state: &State, args: &[String]) -> anyhow::Result<Value> {
    let key = &args[0];
    let value = if let Some(value) = state.map.get(key) {
        if value.expires_at.is_none_or(|e| Instant::now() < e) {
            eprintln!("get {key} from map -> {:?}", value.value);
            match &value.value {
                MapValueContent::Integer(n) => Value::bulk_string(n.to_string()),
                MapValueContent::String(string) => Value::bulk_string(string.clone()),
                MapValueContent::List(_) => Value::Null,
                MapValueContent::Stream(_) => Value::Null,
            }
        } else {
            drop(value);
            state.map.remove(key);
            eprintln!("remove {key} from map because expired");
            Value::Null
        }
    } else {
        eprintln!("get {key} from map -> (nil)");
        Value::Null
    };

    Ok(value)
}
