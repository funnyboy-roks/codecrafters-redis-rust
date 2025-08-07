use std::net::SocketAddr;

use tokio::net::{TcpListener, TcpStream};

async fn handle_connection(stream: TcpStream, addr: SocketAddr) -> anyhow::Result<()> {
    println!("accepted new connection");
    Ok(())
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let listener = TcpListener::bind("127.0.0.1:6379").await?;

    loop {
        let (stream, addr) = listener.accept().await?;
        tokio::spawn(handle_connection(stream, addr));
    }

    Ok(())
}
