use std::{
    collections::{BTreeMap, VecDeque},
    fmt::Display,
    sync::Arc,
};

use anyhow::{bail, ensure, Context};
use command::Command;
use dashmap::DashMap;
use rand::{distr::Alphanumeric, Rng};
use resp::Value;
use tokio::{
    io::{AsyncBufRead, AsyncBufReadExt, AsyncRead, AsyncWrite, BufReader},
    net::{TcpListener, TcpStream},
    sync::{mpsc, oneshot, RwLock},
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
    master_tx: RwLock<Option<mpsc::UnboundedSender<Value>>>,
    replication_id: String,
    replication_offset: u64,
    listening_port: u16,
    replicas: RwLock<Vec<mpsc::UnboundedSender<Value>>>,
}

impl State {
    fn new(role: Role, listening_port: u16) -> Self {
        Self {
            map: Default::default(),
            waiting_on_list: Default::default(),
            waiting_on_stream: Default::default(),
            role,
            master_tx: Default::default(),
            replication_id: rand::rng()
                .sample_iter(Alphanumeric)
                .take(40)
                .map(char::from)
                .collect(),
            replication_offset: 0,
            listening_port,
            replicas: Default::default(),
        }
    }

    pub fn is_replica(&self) -> bool {
        matches!(self.role, Role::Replica(_))
    }

    async fn do_handshake(self: Arc<Self>) -> anyhow::Result<()> {
        let Role::Replica(ref master) = self.role else {
            panic!("this redis server is not a replica!");
        };

        let stream = TcpStream::connect(master).await?;
        let (read, mut write) = stream.into_split();
        let mut read = BufReader::new(read);

        // PING command
        Value::from_iter(["PING"])
            .write_to(&mut write)
            .await
            .context("sending PING in handshake")?;

        let pong = resp::parse(&mut read)
            .await
            .context("reading response to PING command")?;

        ensure!(pong == serde_json::json!("PONG"));
        eprintln!("received pong response from ping command");

        Value::from_iter([
            "REPLCONF",
            "listening-port",
            &self.listening_port.to_string(),
        ])
        .write_to(&mut write)
        .await
        .context("sending first REPLCONF in handshake")?;

        let ok = resp::parse(&mut read)
            .await
            .context("reading response from first REPLCONF command")?;

        ensure!(ok == serde_json::json!("OK"));
        eprintln!("received OK response from first REPLCONF command");

        Value::from_iter(["REPLCONF", "capa", "psync2"])
            .write_to(&mut write)
            .await
            .context("sending second REPLCONF in handshake")?;

        let ok = resp::parse(&mut read)
            .await
            .context("reading response from second REPLCONF command")?;

        ensure!(ok == serde_json::json!("OK"));
        eprintln!("received OK response from second REPLCONF command");

        Value::from_iter(["PSYNC", "?", "-1"])
            .write_to(&mut write)
            .await
            .context("sending PSYNC in handshake")?;

        let ok = resp::parse(&mut read)
            .await
            .context("reading response from PSYNC command")?;

        dbg!(&ok);
        ensure!(ok.as_str().unwrap().starts_with("FULLRESYNC"));
        eprintln!("received FULLRESYNC response from PSYNC command");

        let _rdb = resp::get_rdb(&mut read)
            .await
            .context("reading rdb response from PSYNC command")?;

        let state = Arc::clone(&self);
        tokio::spawn(async move {
            handle_connection(state, read, write, "to master".to_string())
                .await
                .unwrap()
        });

        Ok(())
    }
}

async fn run_command(
    state: &State,
    txn: &mut Option<Vec<Vec<String>>>,
    command: &[String],
    tx: &mpsc::UnboundedSender<Value>,
) -> anyhow::Result<Option<Value>> {
    let (command, args) = command.split_first().expect("command length >= 1");

    let command: Command = command.parse()?;
    eprintln!(
        "[{}:{}:{}] command = {:?}, args = {:?}",
        file!(),
        line!(),
        column!(),
        &command,
        &args
    );

    if command.is_write() {
        state
            .replicas
            .write()
            .await
            .retain(|replica| replica.send(command.into_command_value(args)).is_ok());
    }

    let ret = command.execute(state, txn, args, tx).await?;

    eprintln!("[{}:{}:{}] ret = {:?}", file!(), line!(), column!(), &ret);

    if command.send_response() {
        eprintln!("send_response is true");
        return Ok(Some(ret));
    }

    if state.is_replica()
        && state
            .master_tx
            .read()
            .await
            .as_ref()
            .is_some_and(|mtx| mtx.same_channel(tx))
    {
        eprintln!("replicating on master");
        return Ok(None);
    }

    Ok(Some(ret))
}

async fn handle_connection<R, W>(
    state: Arc<State>,
    read: R,
    mut write: W,
    addr: String,
) -> anyhow::Result<()>
where
    R: AsyncBufRead + Unpin + Send + 'static,
    W: AsyncWrite + Unpin,
{
    println!("accepted new connection: {addr}");

    let (tx, mut rx) = mpsc::unbounded_channel::<Value>();

    async fn read_commands<R>(
        mut r: R,
        state: Arc<State>,
        tx: mpsc::UnboundedSender<Value>,
    ) -> anyhow::Result<()>
    where
        R: AsyncRead + AsyncBufRead + Unpin,
    {
        let mut txn: Option<Vec<Vec<String>>> = None;
        loop {
            let filled = r.fill_buf().await.context("filling buf").unwrap();

            if filled.is_empty() {
                return Ok(());
            }

            let value = resp::parse(&mut r)
                .await
                .context("parsing command")
                .unwrap();

            let full_command: Vec<String> =
                serde_json::from_value(value).context("parsing command")?;

            eprintln!(
                "[{}:{}:{}] received command = {:?}",
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
                        ret.push(run_command(&state, &mut new_txn, cmd, &tx).await?.unwrap()); // TODO: don't unwrap
                        assert!(new_txn.is_none());
                    }
                    txn = None;
                    Some(Value::from(ret))
                } else if command.eq_ignore_ascii_case("discard") {
                    txn = None;
                    Some(Value::simple_string("OK"))
                } else {
                    txn_inner.push(full_command.clone());
                    Some(Value::simple_string("QUEUED"))
                }
            } else {
                run_command(&state, &mut txn, &full_command, &tx).await?
            };

            if let Some(ret) = ret {
                tx.send(ret)
                    .with_context(|| format!("responding to {:?} command", full_command.first()))?;
            }
        }
    }

    if state.is_replica() {
        *state.master_tx.write().await = Some(tx.clone());
    }

    let read_cmd_handle = tokio::spawn(read_commands(read, Arc::clone(&state), tx));

    while let Some(value) = rx.recv().await {
        eprintln!(
            "[{}:{}:{}] sending value    = {:?}",
            file!(),
            line!(),
            column!(),
            &value
        );
        value
            .write_to(&mut write)
            .await
            .with_context(|| format!("sending value: {value:?}"))?;
    }

    if !read_cmd_handle.is_finished() {
        eprintln!("Waiting for 'read_cmd_handle' to be finished");
        read_cmd_handle.await??;
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

    if state.is_replica() {
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
            let (read, write) = stream.into_split();
            let read = BufReader::new(read);
            match handle_connection(state, read, write, format!("from {addr}")).await {
                Ok(()) => {}
                Err(err) => eprintln!("Error handling connection: {err:?}"),
            }
        });
    }
}
