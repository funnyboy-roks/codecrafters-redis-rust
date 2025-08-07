use std::net::SocketAddr;

use anyhow::{bail, Context};
use tokio::{
    io::{AsyncBufReadExt, AsyncWriteExt, BufReader},
    net::{TcpListener, TcpStream},
};

pub mod resp;

async fn handle_connection(mut stream: TcpStream, addr: SocketAddr) -> anyhow::Result<()> {
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
    let listener = TcpListener::bind("127.0.0.1:6379").await?;

    loop {
        let (stream, addr) = listener.accept().await?;
        tokio::spawn(async move {
            match handle_connection(stream, addr).await {
                Ok(()) => {}
                Err(err) => eprintln!("Error handling connection: {err:?}"),
            }
        });
    }
}
