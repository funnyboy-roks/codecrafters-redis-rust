use std::{
    collections::BTreeMap,
    ops::Bound,
    sync::Arc,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use anyhow::{bail, Context};
use tokio::{
    sync::{mpsc, Mutex},
    task::JoinSet,
};

use crate::{resp::Value, MapValue, MapValueContent, State, StreamEvent};

pub async fn ty(state: &State, args: &[String]) -> anyhow::Result<Option<Value>> {
    let [key, ..] = args else {
        todo!("args.len() < 1");
    };

    let kind = if let Some(val) = state.map.get(key) {
        match val.value {
            MapValueContent::String(_) | MapValueContent::Integer(_) => "string",
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
            MapValueContent::String(_) | MapValueContent::Integer(_) => todo!(),
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
            MapValueContent::String(_) | MapValueContent::Integer(_) => todo!(),
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

    if let Some(mut txs) = state.waiting_on_stream.get_mut(key) {
        txs.retain(|tx| {
            tx.send(StreamEvent {
                id,
                kv_pairs: kv_pairs.into(),
            })
            .is_ok()
        });
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
            MapValueContent::String(_) | MapValueContent::Integer(_) => todo!(),
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

async fn xread_streams(state: &State, streams: &[String]) -> anyhow::Result<Option<Value>> {
    assert_eq!(streams.len() % 2, 0);

    let (keys, starts) = streams.split_at(streams.len() / 2);

    assert_eq!(keys.len(), starts.len());

    let mut ret = Vec::with_capacity(keys.len());

    for (key, start) in keys.iter().zip(starts) {
        if let Some(x) = state.map.get(key) {
            match x.value {
                MapValueContent::String(_) | MapValueContent::Integer(_) => todo!(),
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

async fn xread_block(state: &State, args: &[String]) -> anyhow::Result<Option<Value>> {
    let [timeout, streams_str, streams @ ..] = args else {
        todo!("args.len() < 3");
    };
    assert_eq!(streams_str, "streams");

    let timeout = Duration::from_millis(timeout.parse().context("invalid timeout provided")?);

    let (keys, starts) = streams.split_at(streams.len() / 2);

    assert_eq!(keys.len(), starts.len());

    let ret = Arc::new(Mutex::new(Vec::<(String, Vec<Value>)>::with_capacity(
        if timeout.is_zero() { 1 } else { keys.len() },
    )));

    let mut jset = JoinSet::new();
    for (key, start) in keys.iter().zip(starts) {
        let (tx, rx) = mpsc::unbounded_channel();
        state
            .waiting_on_stream
            .entry(key.clone())
            .or_default()
            .push(tx);

        let ret = Arc::clone(&ret);

        let key = key.clone();
        let start = if start == "$" {
            None
        } else {
            Some(parse_id(
                start.split_once('-').context("id should be correct")?,
            )?)
        };

        let fut = async move {
            let mut rx = rx;
            while let Some(StreamEvent { id, kv_pairs }) = rx.recv().await {
                let mut ret = ret.lock().await;
                let idx = ret
                    .iter()
                    .enumerate()
                    .find_map(|(i, v)| (v.0 == *key).then_some(i));

                if start.is_some_and(|start| id <= start) {
                    continue;
                }

                let new = Value::from_iter([id_to_value(id), Value::from_iter(kv_pairs)]);

                if let Some(idx) = idx {
                    ret[idx].1.push(new);
                } else {
                    ret.push((key.clone(), vec![new]));
                    if timeout.is_zero() {
                        return;
                    }
                }
            }
        };
        if timeout.is_zero() {
            jset.spawn(fut);
        } else {
            jset.spawn(async move {
                let _ = tokio::time::timeout(timeout, fut).await;
            });
        }
    }

    while let Some(x) = jset.join_next().await {
        x?;
        if timeout.is_zero() {
            break;
        }
    }

    let ret = Arc::into_inner(ret)
        .expect("Everything dropped since the futures are done")
        .into_inner();

    Ok(Some(if ret.is_empty() {
        Value::Null
    } else {
        ret.into_iter()
            .map(|(k, v)| Value::from_iter([Value::from(k), Value::from(v)]))
            .collect()
    }))
}

pub async fn xread(state: &State, args: &[String]) -> anyhow::Result<Option<Value>> {
    match &*args[0] {
        "streams" => xread_streams(state, &args[1..]).await,
        "block" => xread_block(state, &args[1..]).await,
        subcmd => bail!("Unknown subcommand '{subcmd}'"),
    }
}
