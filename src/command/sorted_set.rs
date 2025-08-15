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

    let count = set
        .replace(SetEntry {
            score: score
                .parse()
                .with_context(|| format!("parsing score '{score}'"))?,
            value: value.clone(),
        })
        .map(|_| 0)
        .unwrap_or(1);

    Ok(Value::from(count))
}

pub async fn zrank(
    state: Arc<State>,
    _: &mut ConnectionState,
    args: &[String],
) -> anyhow::Result<Value> {
    let [key, value] = args else {
        todo!("args.len() != 2");
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

    let ret = if let Some((i, _value)) = set.iter().enumerate().find(|(_, v)| v.value == *value) {
        Value::from(i)
    } else {
        Value::Null
    };

    Ok(ret)
}
