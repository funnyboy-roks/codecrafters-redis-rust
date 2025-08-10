use anyhow::bail;

use crate::{resp::Value, MapValueContent, State};

pub async fn incr(state: &State, args: &[String]) -> anyhow::Result<Option<Value>> {
    let [key, ..] = args else {
        bail!("TODO: args.len() < 1");
    };

    let value = if let Some(mut x) = state.map.get_mut(key) {
        match x.value {
            MapValueContent::String(ref mut val) => {
                if let Ok(num) = val.parse::<i64>() {
                    let next = num + 1;
                    *val = next.to_string();
                    Value::from(next)
                } else {
                    todo!("3/3");
                }
            }
            MapValueContent::List(_) => todo!("3/3"),
            MapValueContent::Stream(_) => todo!("3/3"),
        }
    } else {
        todo!("2/3")
    };

    Ok(Some(value))
}
