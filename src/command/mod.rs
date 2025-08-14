use std::{
    fmt::Display,
    str::FromStr,
    sync::Arc,
    time::{Duration, SystemTime},
};

use anyhow::{bail, Context};
use serde::Deserialize;

use crate::{resp::Value, ConnectionMode, ConnectionState, MapValue, MapValueContent, State};

pub mod list;
pub mod persistence;
pub mod pubsub;
pub mod replication;
pub mod stream;
pub mod transaction;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
pub enum Command {
    Ping,
    Echo,
    Set,
    Get,

    RPush,
    LPush,
    LRange,
    LLen,
    LPop,
    BLPop,

    Type,
    XAdd,
    XRange,
    XRead,

    Incr,
    Multi,
    Exec,
    Discard,

    Info,
    ReplConf,
    PSync,

    Config,
    Keys,

    Subscribe,
}

impl FromStr for Command {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let cmd = match &*s.to_lowercase() {
            "ping" => Self::Ping,
            "echo" => Self::Echo,
            "set" => Self::Set,
            "get" => Self::Get,

            "rpush" => Self::RPush,
            "lpush" => Self::LPush,
            "lrange" => Self::LRange,
            "llen" => Self::LLen,
            "lpop" => Self::LPop,
            "blpop" => Self::BLPop,

            "type" => Self::Type,
            "xadd" => Self::XAdd,
            "xrange" => Self::XRange,
            "xread" => Self::XRead,

            "incr" => Self::Incr,
            "multi" => Self::Multi,
            "exec" => Self::Exec,
            "discard" => Self::Discard,

            "info" => Self::Info,
            "replconf" => Self::ReplConf,
            "psync" => Self::PSync,

            "config" => Self::Config,
            "keys" => Self::Keys,

            "subscribe" => Self::Subscribe,

            _ => {
                bail!("unknown command: {s:?}");
            }
        };
        Ok(cmd)
    }
}

impl Display for Command {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.to_str())
    }
}

impl Command {
    pub const fn to_str(self) -> &'static str {
        match self {
            Self::Ping => "PING",
            Self::Echo => "ECHO",
            Self::Set => "SET",
            Self::Get => "GET",

            Self::RPush => "RPUSH",
            Self::LPush => "LPUSH",
            Self::LRange => "LRANGE",
            Self::LLen => "LLEN",
            Self::LPop => "LPOP",
            Self::BLPop => "BLPOP",

            Self::Type => "TYPE",
            Self::XAdd => "XADD",
            Self::XRange => "XRANGE",
            Self::XRead => "XREAD",

            Self::Incr => "INCR",
            Self::Multi => "MULTI",
            Self::Exec => "EXEC",
            Self::Discard => "DISCARD",

            Self::Info => "INFO",
            Self::ReplConf => "REPLCONF",
            Self::PSync => "PSYNC",

            Self::Config => "CONFIG",
            Self::Keys => "KEYS",

            Self::Subscribe => "SUBSCRIBE",
        }
    }

    pub const fn is_write(self) -> bool {
        match self {
            Command::Ping
            | Command::Echo
            | Command::Get
            | Command::LRange
            | Command::LLen
            | Command::Type
            | Command::XRange
            | Command::XRead
            | Command::Multi
            | Command::Exec
            | Command::Discard
            | Command::Info
            | Command::ReplConf
            | Command::PSync
            | Command::Config
            | Command::Keys
            | Command::Subscribe => false,

            Command::Set
            | Command::RPush
            | Command::LPush
            | Command::LPop
            | Command::BLPop
            | Command::XAdd
            | Command::Incr => true,
        }
    }

    pub const fn send_response(self) -> bool {
        match self {
            Command::ReplConf => true,
            Command::Ping
            | Command::Echo
            | Command::Set
            | Command::Get
            | Command::RPush
            | Command::LPush
            | Command::LRange
            | Command::LLen
            | Command::LPop
            | Command::BLPop
            | Command::Type
            | Command::XAdd
            | Command::XRange
            | Command::XRead
            | Command::Incr
            | Command::Multi
            | Command::Exec
            | Command::Discard
            | Command::Info
            | Command::PSync
            | Command::Config
            | Command::Keys
            | Command::Subscribe => false,
        }
    }

    pub fn into_command_value(self, args: &[String]) -> Value {
        std::iter::once(Value::from(self))
            .chain(args.iter().map(Value::from))
            .collect()
    }

    pub async fn execute(
        self,
        conn_state: &mut ConnectionState,
        args: &[String],
        tx: &tokio::sync::mpsc::UnboundedSender<Value>,
    ) -> anyhow::Result<Value> {
        eprintln!("Command::execute on {self:?}");
        let state = Arc::clone(&conn_state.app_state);
        let ret = match (self, conn_state.mode) {
            // Intro
            (Command::Ping, ConnectionMode::Normal) => {
                Value::simple_string("PONG")
            }
            (Command::Echo, ConnectionMode::Normal) => Value::bulk_string(&args[0]),
            (Command::Set, ConnectionMode::Normal) => set(state, conn_state, args).await?,
            (Command::Get, ConnectionMode::Normal) => get(state, conn_state, args).await?,

            // Lists
            (Command::RPush, ConnectionMode::Normal) => {
                list::rpush(state, conn_state, args).await?
            }
            (Command::LPush, ConnectionMode::Normal) => {
                list::lpush(state, conn_state, args).await?
            }
            (Command::LRange, ConnectionMode::Normal) => {
                list::lrange(state, conn_state, args).await?
            }
            (Command::LLen, ConnectionMode::Normal) => list::llen(state, conn_state, args).await?,
            (Command::LPop, ConnectionMode::Normal) => list::lpop(state, conn_state, args).await?,
            (Command::BLPop, ConnectionMode::Normal) => {
                list::blpop(state, conn_state, args).await?
            }

            // Streams
            (Command::Type, ConnectionMode::Normal) => stream::ty(state, conn_state, args).await?,
            (Command::XAdd, ConnectionMode::Normal) => {
                stream::xadd(state, conn_state, args).await?
            }
            (Command::XRange, ConnectionMode::Normal) => {
                stream::xrange(state, conn_state, args).await?
            }
            (Command::XRead, ConnectionMode::Normal) => {
                stream::xread(state, conn_state, args).await?
            }

            // Transactions
            (Command::Incr, ConnectionMode::Normal) => {
                transaction::incr(state, conn_state, args).await?
            }
            (Command::Multi, ConnectionMode::Normal) => {
                conn_state.txn = Some(Vec::new());
                Value::simple_string("OK")
            }
            (Command::Exec, ConnectionMode::Normal) => {
                Value::simple_error("ERR EXEC without MULTI")
            }
            (Command::Discard, ConnectionMode::Normal) => {
                Value::simple_error("ERR DISCARD without MULTI")
            }

            // Replication
            (Command::Info, ConnectionMode::Normal) => {
                replication::info(state, conn_state, args).await?
            }
            (Command::ReplConf, ConnectionMode::Normal) => {
                replication::replconf(state, conn_state, args, tx).await?
            }
            (Command::PSync, ConnectionMode::Normal) => {
                replication::psync(state, conn_state, args, tx).await?
            }

            // RDB Persistence
            (Command::Config, ConnectionMode::Normal) => {
                persistence::config(state, conn_state, args).await?
            }
            (Command::Keys, ConnectionMode::Normal) => {
                persistence::keys(state, conn_state, args).await?
            }

            (Command::Subscribe, ConnectionMode::Normal | ConnectionMode::Subscribed) => {
                pubsub::subscribe(state, conn_state, args).await?
            }
            (Command::Ping, ConnectionMode::Subscribed) => {
                Value::from_iter(["pong", ""])
            }

            (cmd, ConnectionMode::Subscribed) => Value::simple_error(format!("ERR Can't execute '{cmd}': only (P|S)SUBSCRIBE / (P|S)UNSUBSCRIBE / PING / QUIT / RESET are allowed in this context"))
        };

        Ok(ret)
    }
}

impl From<Command> for Value {
    fn from(value: Command) -> Self {
        Value::from(value.to_str())
    }
}

pub async fn set(
    state: Arc<State>,
    _: &mut ConnectionState,
    args: &[String],
) -> anyhow::Result<Value> {
    let [key, value, ..] = args else {
        todo!("args.len() < 2");
    };

    let value = MapValue {
        value: MapValueContent::from(&**value),
        expires_at: if args.len() > 2 && args[2].eq_ignore_ascii_case("px") {
            let ms: u64 = args[3].parse().context("parsing millis until expiration")?;

            Some(SystemTime::now() + Duration::from_millis(ms))
        } else {
            None
        },
    };

    state.map.insert(key.clone(), value);
    Ok(Value::bulk_string("OK"))
}

pub async fn get(
    state: Arc<State>,
    _: &mut ConnectionState,
    args: &[String],
) -> anyhow::Result<Value> {
    let key = &args[0];
    let value = if let Some(value) = state.map.get(key) {
        if value.expires_at.is_none_or(|e| SystemTime::now() < e) {
            eprintln!("get {key} from map -> {:?}", value.value);
            if let Some(expires_at) = value.expires_at {
                let expires_in = expires_at.duration_since(SystemTime::now()).unwrap();
                eprintln!("expires in {}s", expires_in.as_secs_f64());
            }
            match &value.value {
                MapValueContent::Integer(n) => Value::bulk_string(n.to_string()),
                MapValueContent::String(string) => Value::bulk_string(string.clone()),
                MapValueContent::List(_) => Value::Null,
                MapValueContent::Stream(_) => Value::Null,
            }
        } else {
            drop(value);
            state.map.remove(key);
            eprintln!("remove {key} from map because expired");
            Value::Null
        }
    } else {
        eprintln!("get {key} from map -> (nil)");
        Value::Null
    };

    Ok(value)
}
