use std::{
    fmt::Write,
    sync::{atomic::Ordering, Arc},
};

use anyhow::{bail, ensure, Context};
use tokio::sync::mpsc;

use crate::{resp::Value, ConnectionState, State};

pub async fn info(
    state: Arc<State>,
    _: &mut ConnectionState,
    args: &[String],
) -> anyhow::Result<Value> {
    let [section, ..] = args else {
        bail!("TODO: args.len() < 1");
    };
    ensure!(
        section == "replication",
        "Section '{section}' is not implemented."
    );
    let mut s = String::new();
    writeln!(s, "role:{}", state.role).expect("write to string does not fail");
    writeln!(s, "master_replid:{}", state.replication_id).expect("write to string does not fail");
    writeln!(
        s,
        "master_repl_offset:{}",
        state.replication_offset.load(Ordering::SeqCst)
    )
    .expect("write to string does not fail");
    Ok(Value::from(s))
}

pub async fn replconf(
    state: Arc<State>,
    _: &mut ConnectionState,
    args: &[String],
    _tx: &mpsc::UnboundedSender<Value>,
) -> anyhow::Result<Value> {
    let [field, args @ ..] = args else {
        bail!("TODO: args.len() < 1");
    };

    let ret = match &*field.to_lowercase() {
        "listening-port" | "capa" => Value::simple_string("OK"),
        "getack" => {
            ensure!(args[0] == "*", "args[0] == '{}'", args[0]);
            Value::from_iter([
                "REPLCONF",
                "ACK",
                &state.replication_offset.load(Ordering::SeqCst).to_string(),
            ])
        }
        _ => bail!("Field '{field}' is not supported."),
    };

    Ok(ret)
}

pub async fn psync(
    state: Arc<State>,
    _: &mut ConnectionState,
    args: &[String],
    tx: &mpsc::UnboundedSender<Value>,
) -> anyhow::Result<Value> {
    let [replication_id, replication_offset] = args else {
        bail!("TODO: args.len() != 2");
    };

    ensure!(
        replication_id == "?" && replication_offset == "-1",
        "Replication id is not '?', got {replication_id} OR Replication offset is not '-1', got {replication_offset}"
    );

    state.replicas.write().await.push(tx.clone());

    tx.send(Value::simple_string(format!(
        "FULLRESYNC {} {}",
        state.replication_id,
        state.replication_offset.load(Ordering::SeqCst)
    )))
    .context("Sending FULLSYNC response")?;

    Ok(Value::Rdb(include_bytes!("./empty.rdb").to_vec()))
}
