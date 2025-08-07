use std::{net::SocketAddr, sync::Arc};

use anyhow::{bail, Context};
use dashmap::DashMap;
use tokio::{
    io::{AsyncBufReadExt, AsyncWriteExt, BufReader},
    net::{TcpListener, TcpStream},
};

pub mod resp;

struct State {
    map: DashMap<String, String>,
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
        let command: Vec<String> = serde_json::from_value(value).context("parssing command")?;

        eprintln!(
            "[{}:{}:{}] command = {:?}",
            file!(),
            line!(),
            column!(),
            &command
        );
        match &*command[0].to_lowercase() {
            "ping" => {
                tx.write_all(b"+PONG\r\n").await?;
            }
            "echo" => {
                let response = command[1].clone();

                resp::write(&mut tx, serde_json::Value::String(response))
                    .await
                    .context("responding to echo command")?;
            }
            "set" => {
                let key = command[1].clone();
                let value = command[2].clone();

                state.map.insert(key, value);

                resp::write(&mut tx, serde_json::json!("OK"))
                    .await
                    .context("responding to set command")?;
            }
            "get" => {
                let key = &command[1];
                let value = if let Some(value) = state.map.get(key) {
                    serde_json::Value::String(value.clone())
                } else {
                    serde_json::Value::Null
                };

                resp::write(&mut tx, value)
                    .await
                    .context("responding to get command")?;
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
