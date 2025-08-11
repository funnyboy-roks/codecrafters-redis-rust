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
    Ok(Value::from(format!("role:{}", state.role)))
}
