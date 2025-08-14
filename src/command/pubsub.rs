use std::sync::Arc;

use anyhow::bail;

use crate::{resp::Value, ConnectionMode, ConnectionState, State};

pub async fn subscribe(
    _: Arc<State>,
    conn_state: &mut ConnectionState,
    args: &[String],
) -> anyhow::Result<Value> {
    let [channel] = args else {
        bail!("TODO: args.len() != 1");
    };

    conn_state.mode = ConnectionMode::Subscribed;
    conn_state.channels.insert(channel.clone());

    Ok(Value::from_iter([
        Value::from("subscribe"),
        Value::from(channel),
        Value::from(conn_state.channels.len()),
    ]))
}
