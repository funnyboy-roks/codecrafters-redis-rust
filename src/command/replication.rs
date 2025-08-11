use crate::{resp::Value, State};

pub async fn info(state: &State, args: &[String]) -> anyhow::Result<Value> {
    Ok(Value::from(format!("role:{}", state.role)))
}
