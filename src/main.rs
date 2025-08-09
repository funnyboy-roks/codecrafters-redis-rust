use std::{
    collections::{BTreeMap, VecDeque},
    net::SocketAddr,
    sync::Arc,
};

use anyhow::{bail, Context};
use dashmap::DashMap;
use resp::Value;
use tokio::{
    io::{AsyncBufReadExt, BufReader},
    net::{TcpListener, TcpStream},
    sync::{mpsc, oneshot},
    time::Instant,
};

pub mod command;
pub mod resp;

#[derive(Debug, Clone)]
enum MapValueContent {
    String(String),
    List(VecDeque<String>),
    Stream(BTreeMap<(u64, u64), Vec<String>>),
}

#[derive(Debug, Clone)]
struct MapValue {
    value: MapValueContent,
    expires_at: Option<Instant>,
}

struct StreamEvent {
    id: (u64, u64),
    kv_pairs: Vec<String>,
}

#[derive(Debug, Default)]
pub struct State {
    map: DashMap<String, MapValue>,
    waiting_on_list: DashMap<String, VecDeque<oneshot::Sender<String>>>,
    waiting_on_stream: DashMap<String, Vec<mpsc::UnboundedSender<StreamEvent>>>,
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
        assert!(!command.is_empty());

        let (command, args) = command.split_first().expect("command length >= 1");

        let ret = match &*command.to_lowercase() {
            "ping" => Some(Value::simple_string("PONG")),
            "echo" => Some(Value::bulk_string(&args[0])),
            "set" => command::set(&state, args).await?,
            "get" => command::get(&state, args).await?,

            "rpush" => command::list::rpush(&state, args).await?,
            "lpush" => command::list::lpush(&state, args).await?,
            "lrange" => command::list::lrange(&state, args).await?,
            "llen" => command::list::llen(&state, args).await?,
            "lpop" => command::list::lpop(&state, args).await?,
            "blpop" => command::list::blpop(&state, args).await?,

            "type" => command::stream::ty(&state, args).await?,
            "xadd" => command::stream::xadd(&state, args).await?,
            "xrange" => command::stream::xrange(&state, args).await?,
            "xread" => command::stream::xread(&state, args).await?,

            _ => {
                bail!("unknown command: {command:?}");
            }
        };

        if let Some(ret) = ret {
            ret.write_to(&mut tx)
                .await
                .context("responding to echo command")?;
        }
    }

    eprintln!("Connection terminated: {addr}");

    Ok(())
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let state = State::default();
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
