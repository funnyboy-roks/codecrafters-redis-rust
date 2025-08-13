use anyhow::{bail, ensure};

use crate::{resp::Value, State};

pub async fn config(state: &State, args: &[String]) -> anyhow::Result<Value> {
    let [method, fields @ ..] = args else {
        bail!("TODO: args.len() < 1");
    };

    let ret = match &*method.to_lowercase() {
        "get" => fields
            .iter()
            .flat_map(|f| {
                [
                    Value::from(f),
                    match &**f {
                        "dir" => state
                            .dir
                            .as_ref()
                            .map(|p| Value::from(&*p.to_string_lossy()))
                            .unwrap_or_default(),
                        "dbfilename" => state
                            .db_filename
                            .as_ref()
                            .map(Value::from)
                            .unwrap_or_default(),
                        _ => panic!("Unknown field '{f}'"),
                    },
                ]
            })
            .collect(),
        _ => bail!("Unknown config method '{method}'"),
    };

    Ok(ret)
}

pub async fn keys(state: &State, args: &[String]) -> anyhow::Result<Value> {
    let [filter] = args else {
        bail!("TODO: args.len() != 1");
    };
    ensure!(filter == "*");

    Ok(state.map.iter().map(|e| Value::from(e.key())).collect())
}
