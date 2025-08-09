use std::collections::{BTreeMap, HashMap};

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

    let data: HashMap<_, _> = kv_pairs
        .chunks_exact(2)
        .map(|x| (x[0].clone(), x[1].clone()))
        .collect();

    let id = if let Some((millis, seq)) = id_string.split_once('-') {
        let millis = millis.parse().context("millis provided invalid format")?;
        let seq = seq.parse().context("seq provided invalid format")?;

        (millis, seq)
    } else {
        // id = "*"
        todo!();
    };

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
                s.insert(id, data);
            }
        }
    } else {
        state.map.insert(
            key.clone(),
            MapValue {
                value: MapValueContent::Stream(BTreeMap::from_iter([(id, data)])),
                expires_at: None,
            },
        );
    }

    Ok(Some(Value::bulk_string(id_string)))
}
