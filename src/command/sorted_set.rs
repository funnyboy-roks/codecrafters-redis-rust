use std::sync::Arc;

use anyhow::Context;

use crate::{resp::Value, ConnectionState, MapValueContent, SetEntry, State};

pub async fn zadd(
    state: Arc<State>,
    _: &mut ConnectionState,
    args: &[String],
) -> anyhow::Result<Value> {
    let [key, score, value] = args else {
        todo!("args.len() != 3");
    };

    let MapValueContent::SortedSet(ref mut set) = state
        .map
        .entry(key.clone())
        .or_insert(crate::MapValue {
            value: MapValueContent::SortedSet(Default::default()),
            expires_at: None,
        })
        .value
    else {
        todo!()
    };

    set.insert(SetEntry {
        score: score
            .parse()
            .with_context(|| format!("parsing score '{score}'"))?,
        value: value.clone(),
    });

    Ok(Value::from(1))
}
