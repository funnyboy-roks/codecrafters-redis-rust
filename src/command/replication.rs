use std::fmt::Write;

use anyhow::{bail, ensure};

use crate::{resp::Value, State};

pub async fn info(state: &State, args: &[String]) -> anyhow::Result<Value> {
    let [section, ..] = args else {
        bail!("TODO: args.len() < 1");
    };
    ensure!(
        section == "replication",
        "Section {section} is not implemented."
    );
    let mut s = String::new();
    writeln!(s, "role:{}", state.role).expect("write to string does not fail");
    writeln!(s, "master_replid:{}", state.replication_id).expect("write to string does not fail");
    writeln!(s, "master_repl_offset:{}", state.replication_offset)
        .expect("write to string does not fail");
    Ok(Value::from(s))
}
