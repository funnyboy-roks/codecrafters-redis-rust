use std::{
    collections::BTreeMap,
    ops::Bound,
    time::{SystemTime, UNIX_EPOCH},
};

use anyhow::Context;

use crate::{resp::Value, MapValue, MapValueContent, State};

pub async fn ty(state: &State, args: &[String]) -> anyhow::Result<Option<Value>> {
    let [key, ..] = args else {
        todo!("args.len() < 1");
    };

    let kind = if let Some(val) = state.map.get(key) {
        match val.value {
            MapValueContent::String(_) => "string",
            MapValueContent::List(_) => "list",
            MapValueContent::Stream(_) => "stream",
        }
    } else {
        "none"
    };

    Ok(Some(Value::simple_string(kind)))
}

pub async fn xadd(state: &State, args: &[String]) -> anyhow::Result<Option<Value>> {
    let [key, id_string, kv_pairs @ ..] = args else {
        todo!("args.len() < 2");
    };

    assert!(kv_pairs.len() % 2 == 0);

    let millis: u64 = if id_string == "*" {
        assert_eq!(id_string, "*");
        let millis: u64 = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .context("It's not < 1970")?
            .as_millis()
            .try_into()
            .context("we're 584.9 million years in the future")?;

        millis
    } else {
        let (millis, _) = id_string.split_once('-').context("invalid id")?;
        millis.parse().context("millis provided invalid format")?
    };

    let seq = if let Some(seq) = id_string
        .split_once('-')
        .map(|(_, s)| s)
        .and_then(|s| (s != "*").then_some(s))
    {
        seq.parse().context("seq provided invalid format")?
    } else if let Some(x) = state.map.get(key) {
        match x.value {
            MapValueContent::String(_) => todo!(),
            MapValueContent::List(_) => todo!(),
            MapValueContent::Stream(ref map) => {
                if let Some(last) = map.range(..(millis + 1, 0)).map(|(k, _)| *k).next_back() {
                    if last.0 == millis {
                        last.1 + 1
                    } else {
                        0
                    }
                } else if millis == 0 {
                    1
                } else {
                    0
                }
            }
        }
    } else if millis == 0 {
        1
    } else {
        0
    };

    let id = (millis, seq);

    if id == (0, 0) {
        return Ok(Some(Value::simple_error(
            "ERR The ID specified in XADD must be greater than 0-0",
        )));
    }

    if let Some(mut x) = state.map.get_mut(key) {
        match x.value {
            MapValueContent::String(_) => todo!(),
            MapValueContent::List(_) => todo!(),
            MapValueContent::Stream(ref mut s) => {
                if let Some(last_id) = s.last_key_value().map(|(k, _)| *k) {
                    if id <= last_id {
                        return Ok(Some(Value::simple_error("ERR The ID specified in XADD is equal or smaller than the target stream top item")));
                    }
                }
                s.insert(id, kv_pairs.into());
            }
        }
    } else {
        state.map.insert(
            key.clone(),
            MapValue {
                value: MapValueContent::Stream(BTreeMap::from_iter([(id, kv_pairs.into())])),
                expires_at: None,
            },
        );
    }

    Ok(Some(id_to_value(id)))
}

fn id_to_value(id: (u64, u64)) -> Value {
    Value::bulk_string(format!("{}-{}", id.0, id.1))
}

fn parse_id((millis, seq): (&str, &str)) -> anyhow::Result<(u64, u64)> {
    let millis = millis.parse().context("parsing start millis")?;
    let seq = seq.parse().context("parsing start seq")?;
    Ok((millis, seq))
}

fn parse_bound(
    bound: &str,
    unbounded_symbol: &str,
    default: u64,
) -> anyhow::Result<Bound<(u64, u64)>> {
    Ok(if bound == unbounded_symbol {
        Bound::Unbounded
    } else if let Some(x) = bound.split_once('-') {
        Bound::Included(parse_id(x)?)
    } else {
        Bound::Included((bound.parse().context("parsing bound")?, default))
    })
}

pub async fn xrange(state: &State, args: &[String]) -> anyhow::Result<Option<Value>> {
    let [key, start, end, ..] = args else {
        todo!("args.len() < 3");
    };

    let start = parse_bound(start, "-", 0)?;
    let end = parse_bound(end, "+", u64::MAX)?;

    let ret = if let Some(x) = state.map.get(key) {
        match x.value {
            MapValueContent::String(_) => todo!(),
            MapValueContent::List(_) => todo!(),
            MapValueContent::Stream(ref map) => map
                .range((start, end))
                .map(|(k, v)| Value::from_iter([id_to_value(*k), v.iter().collect()]))
                .collect(),
        }
    } else {
        Value::Null
    };

    Ok(Some(ret))
}

pub async fn xread(state: &State, args: &[String]) -> anyhow::Result<Option<Value>> {
    let [streams_str, streams @ ..] = args else {
        todo!("args.len() < 3");
    };

    assert_eq!(streams_str, "streams");
    assert_eq!(streams.len() % 2, 0);

    let (keys, starts) = streams.split_at(streams.len() / 2);

    assert_eq!(keys.len(), starts.len());

    let mut ret = Vec::with_capacity(keys.len());

    for (key, start) in keys.iter().zip(starts) {
        if let Some(x) = state.map.get(key) {
            match x.value {
                MapValueContent::String(_) => todo!(),
                MapValueContent::List(_) => todo!(),
                MapValueContent::Stream(ref map) => {
                    let start = parse_id(
                        start
                            .split_once('-')
                            .expect("start should always be a valid id 🤞"),
                    )?;
                    ret.push(Value::from_iter([
                        Value::bulk_string(key),
                        map.range((Bound::Excluded(start), Bound::Unbounded))
                            .map(|(k, v)| Value::from_iter([id_to_value(*k), v.iter().collect()]))
                            .collect(),
                    ]));
                }
            }
        }
    }

    Ok(Some(Value::from(ret)))
}
