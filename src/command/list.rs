use std::time::Duration;

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

                if let Some((_, waiting)) = state.waiting_on_list.remove(key) {
                    if let Err(e) =
                        waiting.send(items.pop_front().expect("pushed at least one item"))
                    {
                        items.push_front(e);
                    }
                }

                len
            }
        }
    } else {
        let mut values = values;

        let removed = if let Some((_, waiting)) = state.waiting_on_list.remove(key) {
            let (first, new_values) = values.split_first().expect("length asserted above");
            if waiting.send(first.clone()).is_ok() {
                values = new_values;
            }
            1
        } else {
            0
        };

        state.map.insert(
            key.clone(),
            MapValue {
                value: MapValueContent::List(values.iter().map(String::clone).collect()),
                expires_at: None,
            },
        );
        values.len() + removed
    };

    Ok(Some(serde_json::Value::from(len)))
}

pub async fn lpush(state: &State, args: &[String]) -> anyhow::Result<Option<serde_json::Value>> {
    let (key, mut values) = args.split_first().expect("TODO: args.len() < 2");

    assert!(!values.is_empty());

    let removed = if let Some((_, waiting)) = state.waiting_on_list.remove(key) {
        let (last, new_values) = values.split_last().expect("length asserted above");
        if waiting.send(last.clone()).is_ok() {
            values = new_values;
        }
        1
    } else {
        0
    };

    let len = if let Some(mut list) = state.map.get_mut(key) {
        match list.value {
            MapValueContent::String(_) => todo!(),
            MapValueContent::List(ref mut items) => {
                items.reserve(values.len());
                values
                    .iter()
                    .map(String::clone)
                    .for_each(|v| items.push_front(v));
                items.len() + removed
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
        values.len() + removed
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
        .map(|n| (n > 0).then_some(n).map(Duration::from_secs))
        .flatten();

    let ret = if let Some(mut list) = state.map.get_mut(key) {
        match list.value {
            MapValueContent::String(_) => todo!(),
            MapValueContent::List(ref mut items) => {
                if let Some(v) = items.pop_front() {
                    serde_json::json!([key, v])
                } else {
                    drop(list);
                    if state.waiting_on_list.contains_key(key) {
                        serde_json::Value::Null
                    } else {
                        let (tx, rx) = tokio::sync::oneshot::channel();
                        state.waiting_on_list.insert(key.clone(), tx);

                        let val = rx
                            .await
                            .with_context(|| format!("Waiting for blpop on key '{key}'"))?;

                        serde_json::Value::from(val)
                    }
                }
            }
        }
    } else if state.waiting_on_list.contains_key(key) {
        serde_json::Value::Null
    } else {
        let (tx, rx) = tokio::sync::oneshot::channel();
        state.waiting_on_list.insert(key.clone(), tx);

        let val = if let Some(timeout) = timeout {
            match tokio::time::timeout(timeout, rx).await {
                Ok(val) => val,
                Err(_) => return Ok(Some(serde_json::Value::Null)),
            }
        } else {
            rx.await
        }
        .with_context(|| format!("Waiting for blpop on key '{key}'"))?;

        serde_json::Value::from(val)
    };

    Ok(Some(ret))
}
