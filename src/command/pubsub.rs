use std::sync::Arc;

use anyhow::bail;

use crate::{resp::Value, ConnectionMode, ConnectionState, State};

pub async fn subscribe(
    state: Arc<State>,
    conn_state: &mut ConnectionState,
    args: &[String],
) -> anyhow::Result<Value> {
    let [channel] = args else {
        bail!("TODO: args.len() != 1");
    };

    conn_state.mode = ConnectionMode::Subscribed;
    conn_state.channels.insert(channel.clone());

    state
        .channel_listeners
        .entry(channel.clone())
        .or_default()
        .push(conn_state.tx().clone());

    Ok(Value::from_iter([
        Value::from("subscribe"),
        Value::from(channel),
        Value::from(conn_state.channels.len()),
    ]))
}

pub async fn publish(
    state: Arc<State>,
    _: &mut ConnectionState,
    args: &[String],
) -> anyhow::Result<Value> {
    let [channel, value] = args else {
        bail!("TODO: args.len() != 1");
    };

    let len = if let Some(mut listeners) = state.channel_listeners.get_mut(channel) {
        listeners.retain(|l| {
            l.send(Value::from_iter(["message", channel, value]))
                .is_ok()
        });
        listeners.len()
    } else {
        0
    };

    Ok(Value::from(len))
}
