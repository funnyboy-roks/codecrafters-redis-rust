use std::{
    collections::{BTreeMap, VecDeque},
    fmt::Display,
    net::SocketAddr,
    sync::Arc,
};

use anyhow::{bail, ensure, Context};
use dashmap::DashMap;
use rand::{distr::Alphanumeric, Rng};
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
    Integer(i64),
    String(String),
    List(VecDeque<String>),
    Stream(BTreeMap<(u64, u64), Vec<String>>),
}

impl From<&str> for MapValueContent {
    fn from(value: &str) -> Self {
        if let Ok(num) = value.parse() {
            Self::Integer(num)
        } else {
            Self::String(value.into())
        }
    }
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

#[derive(Debug, Clone, PartialEq, Eq)]
enum Role {
    Master,
    Replica(String),
}

impl Display for Role {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Role::Master => write!(f, "master"),
            Role::Replica(_) => write!(f, "slave"),
        }
    }
}

#[derive(Debug)]
pub struct State {
    map: DashMap<String, MapValue>,
    waiting_on_list: DashMap<String, VecDeque<oneshot::Sender<String>>>,
    waiting_on_stream: DashMap<String, Vec<mpsc::UnboundedSender<StreamEvent>>>,
    role: Role,
    replication_id: String,
    replication_offset: u64,
    listening_port: u16,
}

impl State {
    fn new(role: Role, listening_port: u16) -> Self {
        Self {
            map: Default::default(),
            waiting_on_list: Default::default(),
            waiting_on_stream: Default::default(),
            role,
            replication_id: rand::rng()
                .sample_iter(Alphanumeric)
                .take(40)
                .map(char::from)
                .collect(),
            replication_offset: 0,
            listening_port,
        }
    }

    async fn do_handshake(self: Arc<Self>) -> anyhow::Result<()> {
        let Role::Replica(ref master) = self.role else {
            panic!("this redis server is not a replica!");
        };

        let mut stream = TcpStream::connect(master).await?;
        let (rx, mut tx) = stream.split();
        let mut rx = BufReader::new(rx);

        // PING command
        Value::from_iter(["PING"])
            .write_to(&mut tx)
            .await
            .context("sending PING in handshake")?;

        let pong = resp::parse(&mut rx)
            .await
            .context("reading response to PING command")?;

        ensure!(pong == serde_json::json!("PONG"));
        eprintln!("received pong response from ping command");

        Value::from_iter([
            "REPLCONF",
            "listening-port",
            &self.listening_port.to_string(),
        ])
        .write_to(&mut tx)
        .await
        .context("sending first REPLCONF in handshake")?;

        let ok = resp::parse(&mut rx)
            .await
            .context("reading response from first REPLCONF command")?;

        ensure!(ok == serde_json::json!("OK"));
        eprintln!("received OK response from first REPLCONF command");

        Value::from_iter(["REPLCONF", "capa", "psync2"])
            .write_to(&mut tx)
            .await
            .context("sending second REPLCONF in handshake")?;

        let ok = resp::parse(&mut rx)
            .await
            .context("reading response from second REPLCONF command")?;

        ensure!(ok == serde_json::json!("OK"));
        eprintln!("received OK response from second REPLCONF command");

        Value::from_iter(["PSYNC", "?", "-1"])
            .write_to(&mut tx)
            .await
            .context("sending PSYNC in handshake")?;

        let ok = resp::parse(&mut rx)
            .await
            .context("reading response from PSYNC command")?;

        dbg!(&ok);
        ensure!(ok.as_str().unwrap().starts_with("FULLRESYNC"));
        eprintln!("received FULLRESYNC response from PSYNC command");

        Ok(())
    }
}

async fn run_command(
    state: &State,
    txn: &mut Option<Vec<Vec<String>>>,
    command: &[String],
) -> anyhow::Result<Value> {
    let (command, args) = command.split_first().expect("command length >= 1");

    let ret = match &*command.to_lowercase() {
        "ping" => Value::simple_string("PONG"),
        "echo" => Value::bulk_string(&args[0]),
        "set" => command::set(state, args).await?,
        "get" => command::get(state, args).await?,

        "rpush" => command::list::rpush(state, args).await?,
        "lpush" => command::list::lpush(state, args).await?,
        "lrange" => command::list::lrange(state, args).await?,
        "llen" => command::list::llen(state, args).await?,
        "lpop" => command::list::lpop(state, args).await?,
        "blpop" => command::list::blpop(state, args).await?,

        "type" => command::stream::ty(state, args).await?,
        "xadd" => command::stream::xadd(state, args).await?,
        "xrange" => command::stream::xrange(state, args).await?,
        "xread" => command::stream::xread(state, args).await?,

        "incr" => command::transaction::incr(state, args).await?,
        "multi" => {
            *txn = Some(Vec::new());
            Value::simple_string("OK")
        }
        "exec" => Value::simple_error("ERR EXEC without MULTI"),
        "discard" => Value::simple_error("ERR DISCARD without MULTI"),

        "info" => command::replication::info(state, args).await?,
        "replconf" => command::replication::replconf(state, args).await?,
        "psync" => command::replication::psync(state, args).await?,

        _ => {
            bail!("unknown command: {command:?}");
        }
    };

    Ok(ret)
}

async fn handle_connection(
    state: Arc<State>,
    mut stream: TcpStream,
    addr: SocketAddr,
) -> anyhow::Result<()> {
    println!("accepted new connection: {addr}");

    let (rx, mut tx) = stream.split();
    let mut buf = BufReader::new(rx);

    let mut txn: Option<Vec<Vec<String>>> = None;
    loop {
        let filled = buf.fill_buf().await?;

        if filled.is_empty() {
            break;
        }

        let value = resp::parse(&mut buf).await.context("parsing value")?;
        let full_command: Vec<String> = serde_json::from_value(value).context("parsing command")?;

        eprintln!(
            "[{}:{}:{}] command = {:?}",
            file!(),
            line!(),
            column!(),
            &full_command
        );

        // TODO: handle error
        assert!(!full_command.is_empty());

        let ret = if let Some(ref mut txn_inner) = txn {
            let command = full_command.first().expect("command length >= 1");
            if command.eq_ignore_ascii_case("exec") {
                let mut ret = Vec::with_capacity(txn_inner.len());
                for cmd in txn_inner {
                    let mut new_txn = None;
                    ret.push(run_command(&state, &mut new_txn, cmd).await?);
                    assert!(new_txn.is_none());
                }
                txn = None;
                ret.into()
            } else if command.eq_ignore_ascii_case("discard") {
                txn = None;
                Value::simple_string("OK")
            } else {
                txn_inner.push(full_command.clone());
                Value::simple_string("QUEUED")
            }
        } else {
            run_command(&state, &mut txn, &full_command).await?
        };

        ret.write_to(&mut tx)
            .await
            .with_context(|| format!("responding to {:?} command", full_command.first()))?;
    }

    eprintln!("Connection terminated: {addr}");

    Ok(())
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let mut args = std::env::args();
    let program = args.next().expect("program is required");

    let print_usage = || -> ! {
        eprintln!("Usage: {program} [--port|-p <port>] [--replicaof <hostname port>]");
        std::process::exit(1);
    };

    let mut port = 6379;
    let mut master: Option<String> = None;
    while let Some(arg) = args.next() {
        match &*arg {
            "--port" | "-p" => {
                let Some(port_str) = args.next() else {
                    print_usage();
                };
                port = port_str.parse().context("malformed port")?;
            }
            "--replicaof" => {
                let Some(master_str) = args.next() else {
                    print_usage();
                };
                let (host, port) = master_str
                    .split_once(' ')
                    .context("malformed master server string")?;
                master = Some(format!("{host}:{port}"));
            }
            _ => bail!("Unexpected argument: {arg}"),
        }
    }

    let state = State::new(master.map(Role::Replica).unwrap_or(Role::Master), port);
    let state = Arc::new(state);

    if let Role::Replica(_) = state.role {
        let state = Arc::clone(&state);
        state.do_handshake().await?;
    }

    let addr = format!("127.0.0.1:{port}");
    let listener = TcpListener::bind(&addr).await?;

    eprintln!("Listening for connections at {addr}.");

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
