use std::{
    collections::{BTreeMap, BTreeSet, HashSet, VecDeque},
    fmt::Display,
    net::SocketAddr,
    path::PathBuf,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
    time::SystemTime,
};

use anyhow::{bail, ensure, Context};
use command::Command;
use dashmap::DashMap;
use rand::{distr::Alphanumeric, Rng};
use resp::Value;
use tokio::{
    fs::File,
    io::{AsyncBufRead, AsyncBufReadExt, AsyncRead, AsyncWrite, BufReader},
    net::{TcpListener, TcpStream},
    sync::{mpsc, oneshot, RwLock},
};

pub mod command;
pub mod rdb;
pub mod resp;

#[derive(Debug, Clone)]
struct SetEntry {
    score: f64,
    value: String,
}

impl PartialEq for SetEntry {
    fn eq(&self, other: &Self) -> bool {
        self.value.eq(&other.value)
    }
}

impl Eq for SetEntry {}

impl PartialOrd for SetEntry {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for SetEntry {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        use std::cmp::Ordering;
        if self.value == other.value {
            return Ordering::Equal;
        }
        match self.score.partial_cmp(&other.score) {
            Some(Ordering::Equal) => self.value.cmp(&other.value),
            ord => ord
                .with_context(|| format!("can't compare floats {} and {}", self.score, other.score))
                .unwrap(),
        }
    }
}

#[derive(Debug, Clone)]
enum MapValueContent {
    Integer(i64),
    String(String),
    List(VecDeque<String>),
    Stream(BTreeMap<(u64, u64), Vec<String>>),
    SortedSet(BTreeSet<SetEntry>),
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
    expires_at: Option<SystemTime>,
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
    replication_offset: AtomicUsize,
    listening_port: u16,
    replicas: RwLock<Vec<mpsc::UnboundedSender<Value>>>,

    channel_listeners: DashMap<String, Vec<mpsc::UnboundedSender<Value>>>,

    dir: Option<PathBuf>,
    db_filename: Option<String>,
}

impl State {
    fn new(
        role: Role,
        listening_port: u16,
        dir: Option<PathBuf>,
        db_filename: Option<String>,
    ) -> Self {
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
            replication_offset: Default::default(),
            listening_port,
            replicas: Default::default(),
            channel_listeners: Default::default(),
            dir,
            db_filename,
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

        let (pong, _) = resp::parse(&mut read)
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

        let (ok, _) = resp::parse(&mut read)
            .await
            .context("reading response from first REPLCONF command")?;

        ensure!(ok == serde_json::json!("OK"));
        eprintln!("received OK response from first REPLCONF command");

        Value::from_iter(["REPLCONF", "capa", "psync2"])
            .write_to(&mut write)
            .await
            .context("sending second REPLCONF in handshake")?;

        let (ok, _) = resp::parse(&mut read)
            .await
            .context("reading response from second REPLCONF command")?;

        ensure!(ok == serde_json::json!("OK"));
        eprintln!("received OK response from second REPLCONF command");

        Value::from_iter(["PSYNC", "?", "-1"])
            .write_to(&mut write)
            .await
            .context("sending PSYNC in handshake")?;

        let (ok, _) = resp::parse(&mut read)
            .await
            .context("reading response from PSYNC command")?;

        dbg!(&ok);
        ensure!(ok.as_str().unwrap().starts_with("FULLRESYNC"));
        eprintln!("received FULLRESYNC response from PSYNC command");

        let _rdb = resp::get_rdb(&mut read)
            .await
            .context("reading rdb response from PSYNC command")?;

        let state = Arc::clone(&self);
        let conn = ConnectionState::new(None, state);
        tokio::spawn(async move { conn.handle_connection(read, write).await.unwrap() });

        Ok(())
    }
}

#[derive(Debug, Clone, Copy, Default)]
pub enum ConnectionMode {
    #[default]
    Normal,
    Subscribed,
}

#[derive(Debug)]
pub struct ConnectionState {
    addr: Option<SocketAddr>,
    txn: Option<Vec<Vec<String>>>,
    channels: HashSet<String>,
    app_state: Arc<State>,
    mode: ConnectionMode,
    tx: Option<mpsc::UnboundedSender<Value>>,
}

impl ConnectionState {
    pub fn new(addr: Option<SocketAddr>, app_state: Arc<State>) -> Self {
        Self {
            addr,
            txn: None,
            channels: Default::default(),
            app_state,
            mode: Default::default(),
            tx: None,
        }
    }

    pub fn is_master(&self) -> bool {
        self.addr.is_none()
    }

    pub fn tx(&self) -> &mpsc::UnboundedSender<Value> {
        // TODO: this unwrap hurts me
        self.tx.as_ref().unwrap()
    }

    pub fn unsubscribe(&mut self, channel: &str) -> usize {
        self.channels.remove(channel);
        let tx = self.tx();
        if let Some(mut channels) = self.app_state.channel_listeners.get_mut(channel) {
            if let Some(idx) = channels
                .iter()
                .enumerate()
                .find_map(|(i, c)| (c.same_channel(tx)).then_some(i))
            {
                channels.swap_remove(idx);
            }
        }

        let len = self.channels.len();
        if len == 0 {
            self.mode = ConnectionMode::Normal;
        }
        len
    }

    pub fn unsubscribe_all(&mut self) {
        let tx = self.tx();
        for channel in &self.channels {
            if let Some(mut channels) = self.app_state.channel_listeners.get_mut(channel) {
                if let Some(idx) = channels
                    .iter()
                    .enumerate()
                    .find_map(|(i, c)| (!c.same_channel(tx)).then_some(i))
                {
                    channels.swap_remove(idx);
                }
            }
        }
    }

    async fn run_command(&mut self, command: &[String]) -> anyhow::Result<Option<Value>> {
        let (command, args) = command.split_first().expect("command length >= 1");

        let command: Command = command.to_uppercase().parse().context("parsing command")?;

        if command.is_write() {
            self.app_state
                .replicas
                .write()
                .await
                .retain(|replica| replica.send(command.into_command_value(args)).is_ok());
        }

        let ret = command.execute(self, args).await?;

        if command.send_response() {
            eprintln!("send_response is true");
            return Ok(Some(ret));
        }

        if self.app_state.is_replica() && self.is_master() {
            eprintln!("skipping response on master");
            return Ok(None);
        }

        Ok(Some(ret))
    }

    async fn read_commands<R>(&mut self, mut r: R) -> anyhow::Result<()>
    where
        R: AsyncRead + AsyncBufRead + Unpin,
    {
        loop {
            let filled = r.fill_buf().await.context("filling buf").unwrap();

            if filled.is_empty() {
                return Ok(());
            }

            let (value, bytes) = resp::parse(&mut r)
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

            let ret = if let Some(ref mut txn_inner) = self.txn {
                let command = full_command.first().expect("command length >= 1");
                if command.eq_ignore_ascii_case("exec") {
                    let mut ret = Vec::with_capacity(txn_inner.len());
                    let txn_inner = self.txn.take().unwrap();
                    for cmd in txn_inner {
                        // TODO: don't unwrap
                        ret.push(self.run_command(&cmd).await?.unwrap());
                    }
                    self.txn = None;
                    Some(Value::from(ret))
                } else if command.eq_ignore_ascii_case("discard") {
                    self.txn = None;
                    Some(Value::simple_string("OK"))
                } else {
                    txn_inner.push(full_command.clone());
                    Some(Value::simple_string("QUEUED"))
                }
            } else {
                self.run_command(&full_command).await?
            };

            if self.is_master() {
                self.app_state
                    .replication_offset
                    .fetch_add(bytes, Ordering::SeqCst);
            }

            if let Some(ret) = ret {
                self.tx()
                    .send(ret)
                    .with_context(|| format!("responding to {:?} command", full_command.first()))?;
            }
        }
    }

    async fn handle_connection<R, W>(mut self, read: R, mut write: W) -> anyhow::Result<()>
    where
        R: AsyncBufRead + Unpin + Send + 'static,
        W: AsyncWrite + Unpin,
    {
        if let Some(addr) = &self.addr {
            eprintln!("accepted new connection: {addr}");
        } else {
            eprintln!("accepted new connection with master");
        }

        let (tx, mut rx) = mpsc::unbounded_channel::<Value>();
        self.tx = Some(tx);

        if self.app_state.is_replica() {
            *self.app_state.master_tx.write().await = Some(self.tx().clone());
        }

        let addr = self.addr;
        let read_cmd_handle =
            tokio::spawn(async move { self.read_commands(read).await.map(|_| self) });

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
        }
        let mut this = read_cmd_handle.await??;

        this.unsubscribe_all();

        if let Some(addr) = addr {
            eprintln!("Connection terminated: {addr}");
        } else {
            eprintln!("Connection with master terminated");
        }

        Ok(())
    }
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
    let mut dir: Option<PathBuf> = None;
    let mut db_filename: Option<String> = None;
    while let Some(arg) = args.next() {
        match &*arg {
            "--port" | "-p" => {
                let Some(port_str) = args.next() else {
                    print_usage();
                };
                port = port_str.parse().context("malformed port")?;
            }
            "--replicaof" if master.is_none() => {
                let Some(master_str) = args.next() else {
                    print_usage();
                };
                let (host, port) = master_str
                    .split_once(' ')
                    .context("malformed master server string")?;
                master = Some(format!("{host}:{port}"));
            }
            "--dir" if dir.is_none() => {
                let Some(dir_str) = args.next() else {
                    print_usage();
                };
                dir = Some(PathBuf::from(dir_str));
            }
            "--dbfilename" if db_filename.is_none() => {
                let Some(name) = args.next() else {
                    print_usage();
                };
                db_filename = Some(name);
            }
            _ => bail!("Unexpected argument: {arg}"),
        }
    }

    let mut state = State::new(
        master.map(Role::Replica).unwrap_or(Role::Master),
        port,
        dir.clone(),
        db_filename.clone(),
    );

    if let Some(ref dir) = dir {
        let path = dir.join(
            db_filename
                .as_ref()
                .expect("dir and dbfilename should both be specified"),
        );
        if tokio::fs::try_exists(&path)
            .await
            .with_context(|| format!("checking where {} exists", path.display()))?
        {
            let db_file = File::open(path).await.context("opening db file")?;
            let mut db_file = BufReader::new(db_file);
            rdb::read(&mut db_file, &mut state)
                .await
                .context("parsing db file")?;
        }
    }

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
            let connection = ConnectionState::new(Some(addr), state);
            match connection.handle_connection(read, write).await {
                Ok(()) => {}
                Err(err) => eprintln!("Error handling connection: {err:?}"),
            }
        });
    }
}
