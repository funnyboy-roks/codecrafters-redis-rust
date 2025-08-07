use std::net::SocketAddr;

use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
};

async fn handle_connection(mut stream: TcpStream, _addr: SocketAddr) -> anyhow::Result<()> {
    println!("accepted new connection");

    let (mut rx, mut tx) = stream.split();
    // let buf = BufReader::new(rx);
    // let mut lines = buf.lines();
    let mut s = String::new();
    rx.read_to_string(&mut s).await?;
    dbg!(s);

    tx.write_all(b"+PONG\r\n").await?;
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
