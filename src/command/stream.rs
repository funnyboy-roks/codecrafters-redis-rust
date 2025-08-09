use std::collections::HashMap;

use crate::{resp::Value, MapValueContent, State};

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
    let [key, entry_id, kv_pairs @ ..] = args else {
        todo!("args.len() < 2");
    };

    assert!(kv_pairs.len() % 2 == 0);

    let mut obj: HashMap<_, _> = kv_pairs
        .chunks_exact(2)
        .map(|x| (x[0].clone(), x[1].clone()))
        .collect();

    obj.insert("id".into(), entry_id.clone());

    if let Some(mut x) = state.map.get_mut(key) {
        match x.value {
            MapValueContent::String(_) => todo!(),
            MapValueContent::List(_) => todo!(),
            MapValueContent::Stream(ref mut s) => s.push(obj),
        }
    } else {
        state.map.insert(
            key.clone(),
            crate::MapValue {
                value: MapValueContent::Stream(vec![obj]),
                expires_at: None,
            },
        );
    }

    Ok(Some(Value::bulk_string(entry_id)))
}
