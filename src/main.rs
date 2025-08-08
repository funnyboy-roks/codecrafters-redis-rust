use std::{net::SocketAddr, sync::Arc, time::Duration};

use anyhow::{bail, Context};
use dashmap::DashMap;
use tokio::{
    io::{AsyncBufReadExt, AsyncWriteExt, BufReader},
    net::{TcpListener, TcpStream},
    time::Instant,
};

pub mod resp;

#[derive(Debug, Clone)]
enum MapValueContent {
    String(String),
    List(Vec<String>),
}

#[derive(Debug, Clone)]
struct MapValue {
    value: MapValueContent,
    expires_at: Option<Instant>,
}

#[derive(Debug, Clone)]
struct State {
    map: DashMap<String, MapValue>,
}

async fn handle_connection(
    state: Arc<State>,
    mut stream: TcpStream,
    addr: SocketAddr,
) -> anyhow::Result<()> {
    println!("accepted new connection: {addr}");

    let (rx, mut tx) = stream.split();
    let mut buf = BufReader::new(rx);

    loop {
        let filled = buf.fill_buf().await?;

        if filled.is_empty() {
            break;
        }

        let value = resp::parse(&mut buf).await.context("parsing value")?;
        let command: Vec<String> = serde_json::from_value(value).context("parsing command")?;

        eprintln!(
            "[{}:{}:{}] command = {:?}",
            file!(),
            line!(),
            column!(),
            &command
        );

        // TODO: handle error
        assert!(command.len() >= 1);

        let (command, args) = command.split_first().expect("command length >= 1");

        match &*command.to_lowercase() {
            "ping" => {
                tx.write_all(b"+PONG\r\n").await?;
            }
            "echo" => {
                let response = args[0].clone();

                resp::write(&mut tx, serde_json::Value::String(response))
                    .await
                    .context("responding to echo command")?;
            }
            "set" => {
                let [key, value, ..] = args else {
                    todo!("args.len() < 2");
                };

                let value = MapValue {
                    value: MapValueContent::String(value.clone()),
                    expires_at: if args.len() > 2 && args[2].eq_ignore_ascii_case("px") {
                        let ms: u64 = args[3].parse().context("parsing millis until expiration")?;

                        Some(Instant::now() + Duration::from_millis(ms))
                    } else {
                        None
                    },
                };

                state.map.insert(key.clone(), value);

                resp::write(&mut tx, serde_json::json!("OK"))
                    .await
                    .context("responding to set command")?;
            }
            "get" => {
                let key = &args[0];
                let value = if let Some(value) = state.map.get(key) {
                    if value.expires_at.is_none_or(|e| Instant::now() < e) {
                        eprintln!("get {key} from map -> {:?}", value.value);
                        match &value.value {
                            MapValueContent::String(string) => {
                                serde_json::Value::String(string.clone())
                            }
                            MapValueContent::List(_) => serde_json::Value::Null,
                        }
                    } else {
                        drop(value);
                        state.map.remove(key);
                        eprintln!("remove {key} from map because expired");
                        serde_json::Value::Null
                    }
                } else {
                    eprintln!("get {key} from map -> (nil)");
                    serde_json::Value::Null
                };

                resp::write(&mut tx, value)
                    .await
                    .context("responding to get command")?;
            }
            "rpush" => {
                let (key, values) = args.split_first().expect("TODO: args.len() < 2");

                let len = if let Some(mut list) = state.map.get_mut(key) {
                    match list.value {
                        MapValueContent::String(_) => todo!(),
                        MapValueContent::List(ref mut items) => {
                            items.extend_from_slice(values);
                            items.len()
                        }
                    }
                } else {
                    state.map.insert(
                        key.clone(),
                        MapValue {
                            value: MapValueContent::List(values.to_vec()),
                            expires_at: None,
                        },
                    );
                    values.len()
                };

                resp::write(
                    &mut tx,
                    serde_json::Value::Number(
                        serde_json::Number::from_u128(len as _)
                            .expect("len is probably <= u64::MAX"),
                    ),
                )
                .await
                .context("responding to rpush command")?;
            }
            "lrange" => {
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
                                    items[start_index..=end_index]
                                        .iter()
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

                resp::write(&mut tx, ret)
                    .await
                    .context("responding to lrange command")?;
            }
            _ => {
                bail!("unknown command: {command:?}");
            }
        }
    }

    eprintln!("Connection terminated: {addr}");

    Ok(())
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let state = State {
        map: Default::default(),
    };
    let state = Arc::new(state);
    let listener = TcpListener::bind("127.0.0.1:6379").await?;

    loop {
        let (stream, addr) = listener.accept().await?;
        let state = Arc::clone(&state);
        tokio::spawn(async move {
            match handle_connection(state, stream, addr).await {
                Ok(()) => {}
                Err(err) => eprintln!("Error handling connection: {err:?}"),
            }
        });
    }
}
