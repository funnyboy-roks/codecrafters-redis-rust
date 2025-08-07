use std::net::SocketAddr;

use tokio::{
    io::{AsyncBufReadExt, AsyncWriteExt, BufReader},
    net::{TcpListener, TcpStream},
};

async fn handle_connection(mut stream: TcpStream, _addr: SocketAddr) -> anyhow::Result<()> {
    println!("accepted new connection");

    let (rx, mut tx) = stream.split();
    let buf = BufReader::new(rx);
    let mut lines = buf.lines();

    while let Some(_) = lines.next_line().await? {
        tx.write_all(b"+PONG\r\n").await?;
    }
    Ok(())
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let listener = TcpListener::bind("127.0.0.1:6379").await?;

    loop {
        let (stream, addr) = listener.accept().await?;
        tokio::spawn(handle_connection(stream, addr));
    }
}
