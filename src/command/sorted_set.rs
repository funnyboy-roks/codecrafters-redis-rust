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

    let mut removed = false;
    set.retain(|e| {
        if e.value == *value {
            removed = true;
            false
        } else {
            true
        }
    });

    set.insert(SetEntry {
        score: score
            .parse()
            .with_context(|| format!("parsing score '{score}'"))?,
        value: value.clone(),
    });

    Ok(Value::from(if removed { 0 } else { 1 }))
}

pub async fn zrank(
    state: Arc<State>,
    _: &mut ConnectionState,
    args: &[String],
) -> anyhow::Result<Value> {
    let [key, value] = args else {
        todo!("args.len() != 2");
    };

    let map_value = state.map.get(key);
    let set = if let Some(ref value) = map_value {
        if let MapValueContent::SortedSet(ref set) = value.value {
            set
        } else {
            todo!()
        }
    } else {
        return Ok(Value::Null);
    };

    let ret = if let Some((i, _value)) = set.iter().enumerate().find(|(_, v)| v.value == *value) {
        Value::from(i)
    } else {
        Value::Null
    };

    Ok(ret)
}

pub async fn zrange(
    state: Arc<State>,
    _: &mut ConnectionState,
    args: &[String],
) -> anyhow::Result<Value> {
    let [key, min, max] = args else {
        todo!("args.len() != 2");
    };

    let min: isize = min.parse().context("parsing min")?;
    let max: isize = max.parse().context("parsing max")?;

    let map_value = state.map.get(key);
    let set = if let Some(ref value) = map_value {
        if let MapValueContent::SortedSet(ref set) = value.value {
            set
        } else {
            todo!()
        }
    } else {
        return Ok(Value::empty_array());
    };

    let min = if min < 0 {
        set.len().saturating_add_signed(min)
    } else {
        min as usize
    };
    let max = if max < 0 {
        set.len().saturating_add_signed(max)
    } else {
        max as usize
    };

    let ret: Value = set
        .iter()
        .skip(min)
        .take(max - min + 1)
        .map(|e| Value::from(&e.value))
        .collect();

    Ok(ret)
}
