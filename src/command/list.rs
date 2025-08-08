use std::{collections::VecDeque, time::Duration};

use anyhow::Context;

use crate::{MapValue, MapValueContent, State};

pub async fn rpush(state: &State, args: &[String]) -> anyhow::Result<Option<serde_json::Value>> {
    let (key, values) = args.split_first().expect("TODO: args.len() < 2");

    assert!(!values.is_empty());

    let len = if let Some(mut list) = state.map.get_mut(key) {
        match list.value {
            MapValueContent::String(_) => todo!(),
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

    Ok(Some(serde_json::Value::from(len)))
}

pub async fn lpush(state: &State, args: &[String]) -> anyhow::Result<Option<serde_json::Value>> {
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
            MapValueContent::String(_) => todo!(),
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

    Ok(Some(serde_json::Value::from(len)))
}

pub async fn lrange(state: &State, args: &[String]) -> anyhow::Result<Option<serde_json::Value>> {
    let [key, start_index, end_index, ..] = args else {
        todo!("args.len() < 3");
    };

    let start_index: isize = start_index.parse().context("Invalid start index")?;
    let end_index: isize = end_index.parse().context("Invalid end index")?;

    let ret = if let Some(list) = state.map.get(key) {
        match list.value {
            MapValueContent::String(_) => todo!(),
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
                    serde_json::json!([])
                } else {
                    serde_json::Value::Array(
                        items
                            .range(start_index..=end_index)
                            .map(Clone::clone)
                            .map(serde_json::Value::String)
                            .collect(),
                    )
                }
            }
        }
    } else {
        serde_json::json!([])
    };

    Ok(Some(ret))
}

pub async fn llen(state: &State, args: &[String]) -> anyhow::Result<Option<serde_json::Value>> {
    let (key, values) = args.split_first().expect("TODO: args.len() < 2");
    assert_eq!(values.len(), 0);

    let len = if let Some(list) = state.map.get(key) {
        match list.value {
            MapValueContent::String(_) => todo!(),
            MapValueContent::List(ref items) => items.len(),
        }
    } else {
        0
    };

    Ok(Some(serde_json::Value::from(len)))
}

pub async fn lpop(state: &State, args: &[String]) -> anyhow::Result<Option<serde_json::Value>> {
    let (key, values) = args.split_first().expect("TODO: args.len() < 2");

    let count: Option<usize> = values
        .first()
        .map(|v| v.parse().expect("invalid lpop count"));

    let ret = if let Some(mut list) = state.map.get_mut(key) {
        match list.value {
            MapValueContent::String(_) => todo!(),
            MapValueContent::List(ref mut items) => {
                if let Some(count) = count {
                    (0..count)
                        .map(|_| items.pop_front())
                        .map(serde_json::Value::from)
                        .collect()
                } else if let Some(v) = items.pop_front() {
                    serde_json::Value::from(v)
                } else {
                    serde_json::Value::Null
                }
            }
        }
    } else {
        serde_json::Value::Null
    };

    Ok(Some(ret))
}

pub async fn blpop(state: &State, args: &[String]) -> anyhow::Result<Option<serde_json::Value>> {
    let (key, values) = args.split_first().expect("TODO: args.len() < 2");

    let timeout = values
        .first()
        .map(|v| v.parse::<u64>().expect("invalid lpop count"))
        .and_then(|n| (n > 0).then_some(n))
        .map(Duration::from_secs);

    let wait = || async {
        let ret: anyhow::Result<serde_json::Value> = {
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
                    Err(_) => return Ok(serde_json::Value::Null),
                }
            } else {
                rx.await
            }
            .with_context(|| format!("Waiting for blpop on key '{key}'"))?;

            Ok(serde_json::json!([key, val]))
        };
        ret
    };

    let ret = if let Some(mut list) = state.map.get_mut(key) {
        match list.value {
            MapValueContent::String(_) => todo!(),
            MapValueContent::List(ref mut items) => {
                if let Some(v) = items.pop_front() {
                    serde_json::json!([key, v])
                } else {
                    drop(list);
                    wait().await?
                }
            }
        }
    } else {
        wait().await?
    };

    Ok(Some(ret))
}
