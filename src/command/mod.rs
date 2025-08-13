use std::{str::FromStr, time::Duration};

use anyhow::{bail, Context};
use tokio::time::Instant;

use crate::{resp::Value, MapValue, MapValueContent, State};

pub mod list;
pub mod persistence;
pub mod replication;
pub mod stream;
pub mod transaction;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
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

            _ => {
                bail!("unknown command: {s:?}");
            }
        };
        Ok(cmd)
    }
}

impl Command {
    fn to_str(self) -> &'static str {
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
            | Command::Keys => false,

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
            | Command::Keys => false,
        }
    }

    pub fn into_command_value(self, args: &[String]) -> Value {
        std::iter::once(Value::from(self))
            .chain(args.iter().map(Value::from))
            .collect()
    }

    pub async fn execute(
        self,
        state: &State,
        txn: &mut Option<Vec<Vec<String>>>,
        args: &[String],
        tx: &tokio::sync::mpsc::UnboundedSender<Value>,
    ) -> anyhow::Result<Value> {
        eprintln!("Command::execute on {self:?}");
        let ret = match self {
            Command::Ping => Value::simple_string("PONG"),
            Command::Echo => Value::bulk_string(&args[0]),
            Command::Set => set(state, args).await?,
            Command::Get => get(state, args).await?,

            Command::RPush => list::rpush(state, args).await?,
            Command::LPush => list::lpush(state, args).await?,
            Command::LRange => list::lrange(state, args).await?,
            Command::LLen => list::llen(state, args).await?,
            Command::LPop => list::lpop(state, args).await?,
            Command::BLPop => list::blpop(state, args).await?,

            Command::Type => stream::ty(state, args).await?,
            Command::XAdd => stream::xadd(state, args).await?,
            Command::XRange => stream::xrange(state, args).await?,
            Command::XRead => stream::xread(state, args).await?,

            Command::Incr => transaction::incr(state, args).await?,
            Command::Multi => {
                *txn = Some(Vec::new());
                Value::simple_string("OK")
            }
            Command::Exec => Value::simple_error("ERR EXEC without MULTI"),
            Command::Discard => Value::simple_error("ERR DISCARD without MULTI"),

            Command::Info => replication::info(state, args).await?,
            Command::ReplConf => replication::replconf(state, args, tx).await?,
            Command::PSync => replication::psync(state, args, tx).await?,

            Command::Config => persistence::config(state, args).await?,
            Command::Keys => persistence::keys(state, args).await?,
        };

        Ok(ret)
    }
}

impl From<Command> for Value {
    fn from(value: Command) -> Self {
        Value::from(value.to_str())
    }
}

pub async fn set(state: &State, args: &[String]) -> anyhow::Result<Value> {
    let [key, value, ..] = args else {
        todo!("args.len() < 2");
    };

    let value = MapValue {
        value: MapValueContent::from(&**value),
        expires_at: if args.len() > 2 && args[2].eq_ignore_ascii_case("px") {
            let ms: u64 = args[3].parse().context("parsing millis until expiration")?;

            Some(Instant::now() + Duration::from_millis(ms))
        } else {
            None
        },
    };

    state.map.insert(key.clone(), value);
    Ok(Value::bulk_string("OK"))
}

pub async fn get(state: &State, args: &[String]) -> anyhow::Result<Value> {
    let key = &args[0];
    let value = if let Some(value) = state.map.get(key) {
        if value.expires_at.is_none_or(|e| Instant::now() < e) {
            eprintln!("get {key} from map -> {:?}", value.value);
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
