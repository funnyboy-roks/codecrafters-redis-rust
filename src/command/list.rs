use std::{collections::VecDeque, sync::Arc, time::Duration};

use anyhow::Context;

use crate::{resp::Value, ConnectionState, MapValue, MapValueContent, State};

pub async fn rpush(
    state: Arc<State>,
    _: &mut ConnectionState,
    args: &[String],
) -> anyhow::Result<Value> {
    let (key, values) = args.split_first().expect("TODO: args.len() < 2");

    assert!(!values.is_empty());

    let len = if let Some(mut list) = state.map.get_mut(key) {
        match list.value {
            MapValueContent::String(_) | MapValueContent::Integer(_) => todo!(),
            MapValueContent::List(ref mut items) => {
                items.extend(values.iter().map(String::clone));
                let len = items.len();

                if let Some(mut waiting) = state.waiting_on_list.get_mut(key) {
                    loop {
                        let Some(tx) = waiting.pop_front() else {
                            break;
                        };
                        let Some(item) = items.pop_front() else {
                            waiting.push_front(tx);
                            break;
                        };

                        if let Err(e) = tx.send(item) {
                            items.push_front(e);
                        }
                    }
                }

                len
            }
            MapValueContent::Stream(_) => todo!(),
            MapValueContent::SortedSet(_) => todo!(),
        }
    } else {
        let mut values = values;

        let og_len = values.len();

        if let Some(mut waiting) = state.waiting_on_list.get_mut(key) {
            loop {
                let Some(tx) = waiting.pop_front() else {
                    break;
                };
                let Some((item, new_values)) = values.split_first() else {
                    waiting.push_front(tx);
                    break;
                };

                if tx.send(item.clone()).is_ok() {
                    values = new_values;
                }
            }
        }

        state.map.insert(
            key.clone(),
            MapValue {
                value: MapValueContent::List(values.iter().map(String::clone).collect()),
                expires_at: None,
            },
        );
        og_len
    };

    Ok(Value::from(len))
}

pub async fn lpush(
    state: Arc<State>,
    _: &mut ConnectionState,
    args: &[String],
) -> anyhow::Result<Value> {
    let (key, mut values) = args.split_first().expect("TODO: args.len() < 2");

    assert!(!values.is_empty());

    let og_len = values.len();

    if let Some(mut waiting) = state.waiting_on_list.get_mut(key) {
        loop {
            let Some(tx) = waiting.pop_front() else {
                break;
            };
            let Some((item, new_values)) = values.split_last() else {
                waiting.push_front(tx);
                break;
            };

            if tx.send(item.clone()).is_ok() {
                values = new_values;
            }
        }
    }

    let len = if let Some(mut list) = state.map.get_mut(key) {
        match list.value {
            MapValueContent::String(_) | MapValueContent::Integer(_) => todo!(),
            MapValueContent::List(ref mut items) => {
                let len = items.len() + og_len;

                items.reserve(values.len());
                values
                    .iter()
                    .map(String::clone)
                    .for_each(|v| items.push_front(v));

                if let Some(mut waiting) = state.waiting_on_list.get_mut(key) {
                    loop {
                        let Some(tx) = waiting.pop_front() else {
                            break;
                        };
                        let Some(item) = items.pop_front() else {
                            waiting.push_front(tx);
                            break;
                        };

                        if let Err(e) = tx.send(item) {
                            items.push_front(e);
                        }
                    }
                }

                len
            }
            MapValueContent::Stream(_) => todo!(),
            MapValueContent::SortedSet(_) => todo!(),
        }
    } else {
        state.map.insert(
            key.clone(),
            MapValue {
                value: MapValueContent::List(values.iter().rev().map(String::clone).collect()),
                expires_at: None,
            },
        );
        og_len
    };

    Ok(Value::from(len))
}

pub async fn lrange(
    state: Arc<State>,
    _: &mut ConnectionState,
    args: &[String],
) -> anyhow::Result<Value> {
    let [key, start_index, end_index, ..] = args else {
        todo!("args.len() < 3");
    };

    let start_index: isize = start_index.parse().context("Invalid start index")?;
    let end_index: isize = end_index.parse().context("Invalid end index")?;

    let ret = if let Some(list) = state.map.get(key) {
        match list.value {
            MapValueContent::String(_) | MapValueContent::Integer(_) => todo!(),
            MapValueContent::List(ref items) => {
                let start_index = if start_index < 0 {
                    items.len().saturating_add_signed(start_index)
                } else {
                    start_index as usize
                };

                let end_index = if end_index < 0 {
                    items.len().saturating_add_signed(end_index)
                } else if end_index as usize >= items.len() {
                    items.len() - 1
                } else {
                    end_index as usize
                };

                if start_index > end_index || start_index >= items.len() {
                    Value::Array(Vec::new())
                } else {
                    items
                        .range(start_index..=end_index)
                        .map(Value::bulk_string)
                        .collect()
                }
            }
            MapValueContent::Stream(_) => todo!(),
            MapValueContent::SortedSet(_) => todo!(),
        }
    } else {
        Value::Array(Vec::new())
    };

    Ok(ret)
}

pub async fn llen(
    state: Arc<State>,
    _: &mut ConnectionState,
    args: &[String],
) -> anyhow::Result<Value> {
    let (key, values) = args.split_first().expect("TODO: args.len() < 2");
    assert_eq!(values.len(), 0);

    let len = if let Some(list) = state.map.get(key) {
        match list.value {
            MapValueContent::String(_) | MapValueContent::Integer(_) => todo!(),
            MapValueContent::List(ref items) => items.len(),
            MapValueContent::Stream(_) => todo!(),
            MapValueContent::SortedSet(_) => todo!(),
        }
    } else {
        0
    };

    Ok(Value::from(len))
}

pub async fn lpop(
    state: Arc<State>,
    _: &mut ConnectionState,
    args: &[String],
) -> anyhow::Result<Value> {
    let (key, values) = args.split_first().expect("TODO: args.len() < 2");

    let count: Option<usize> = values
        .first()
        .map(|v| v.parse().expect("invalid lpop count"));

    let ret = if let Some(mut list) = state.map.get_mut(key) {
        match list.value {
            MapValueContent::String(_) | MapValueContent::Integer(_) => todo!(),
            MapValueContent::List(ref mut items) => {
                if let Some(count) = count {
                    (0..count)
                        .flat_map(|_| items.pop_front())
                        .map(Value::bulk_string)
                        .collect()
                } else if let Some(v) = items.pop_front() {
                    Value::bulk_string(v)
                } else {
                    Value::Null
                }
            }
            MapValueContent::Stream(_) => todo!(),
            MapValueContent::SortedSet(_) => todo!(),
        }
    } else {
        Value::Null
    };

    Ok(ret)
}

pub async fn blpop(
    state: Arc<State>,
    _: &mut ConnectionState,
    args: &[String],
) -> anyhow::Result<Value> {
    let (key, values) = args.split_first().expect("TODO: args.len() < 2");

    let timeout = values
        .first()
        .map(|v| v.parse::<f64>().expect("invalid lpop count"))
        .and_then(|n| (n > 0.).then_some(n))
        .map(Duration::from_secs_f64);

    let wait = || async {
        let ret: anyhow::Result<Value> = {
            let (tx, rx) = tokio::sync::oneshot::channel();
            if let Some(mut waiting) = state.waiting_on_list.get_mut(key) {
                waiting.push_back(tx);
            } else {
                let mut vd = VecDeque::with_capacity(1);
                vd.push_back(tx);
                state.waiting_on_list.insert(key.into(), vd);
            }

            let val = if let Some(timeout) = timeout {
                match tokio::time::timeout(timeout, rx).await {
                    Ok(val) => val,
                    Err(_) => return Ok(Value::Null),
                }
            } else {
                rx.await
            }
            .with_context(|| format!("Waiting for blpop on key '{key}'"))?;

            Ok(Value::from_iter([key.clone(), val]))
        };
        ret
    };

    let ret = if let Some(mut list) = state.map.get_mut(key) {
        match list.value {
            MapValueContent::String(_) | MapValueContent::Integer(_) => todo!(),
            MapValueContent::List(ref mut items) => {
                if let Some(v) = items.pop_front() {
                    Value::from_iter([key.clone(), v])
                } else {
                    drop(list);
                    wait().await?
                }
            }
            MapValueContent::Stream(_) => todo!(),
            MapValueContent::SortedSet(_) => todo!(),
        }
    } else {
        wait().await?
    };

    Ok(ret)
}
