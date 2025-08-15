use std::sync::Arc;

use anyhow::bail;

use crate::{resp::Value, ConnectionState, MapValue, MapValueContent, State};

pub async fn incr(
    state: Arc<State>,
    _: &mut ConnectionState,
    args: &[String],
) -> anyhow::Result<Value> {
    let [key, ..] = args else {
        bail!("TODO: args.len() < 1");
    };

    let value = if let Some(mut x) = state.map.get_mut(key) {
        match x.value {
            MapValueContent::Integer(ref mut val) => {
                *val += 1;
                Value::from(*val)
            }
            MapValueContent::String(_)
            | MapValueContent::List(_)
            | MapValueContent::Stream(_)
            | MapValueContent::SortedSet(_) => {
                Value::simple_error("ERR value is not an integer or out of range")
            }
        }
    } else {
        state.map.insert(
            key.clone(),
            MapValue {
                value: MapValueContent::Integer(1),
                expires_at: None,
            },
        );
        Value::from(1)
    };

    Ok(value)
}
