use std::collections::{HashMap, HashSet};
use std::hash::Hash;

use anyhow::{bail, ensure, Context};
use tokio::io::{AsyncBufRead, AsyncBufReadExt, AsyncReadExt, AsyncWrite, AsyncWriteExt};

#[derive(Copy, Clone, Debug, PartialEq, Eq)]
#[repr(u8)]
pub enum DataKind {
    SimpleString = b'+',
    SimpleError = b'-',
    Integer = b':',
    BulkString = b'$',
    Array = b'*',
    Boolean = b'#',
    Double = b',',
    BigNumber = b'(',
    BulkError = b'!',
    VerbatimString = b'=',
    Map = b'%',
    Attribute = b'|',
    Set = b'~',
    Push = b'>',
}

impl From<DataKind> for u8 {
    fn from(value: DataKind) -> Self {
        value as _
    }
}

impl TryFrom<u8> for DataKind {
    type Error = anyhow::Error;

    fn try_from(value: u8) -> Result<Self, Self::Error> {
        match value {
            b'+' => Ok(Self::SimpleString),
            b'-' => Ok(Self::SimpleError),
            b':' => Ok(Self::Integer),
            b'$' => Ok(Self::BulkString),
            b'*' => Ok(Self::Array),
            b'#' => Ok(Self::Boolean),
            b',' => Ok(Self::Double),
            b'(' => Ok(Self::BigNumber),
            b'!' => Ok(Self::BulkError),
            b'=' => Ok(Self::VerbatimString),
            b'%' => Ok(Self::Map),
            b'|' => Ok(Self::Attribute),
            b'~' => Ok(Self::Set),
            b'>' => Ok(Self::Push),
            _ => bail!("Unknown datakind symbol: '{}'", value as char),
        }
    }
}

/// Take all the bytes, up to the \r\n and append them to `buf`.  Reads out the \r\n from the
/// reader
async fn take_until_delim<R>(r: &mut R, buf: &mut Vec<u8>) -> anyhow::Result<usize>
where
    R: AsyncBufRead + Unpin,
{
    let mut bytes = 0;
    bytes += r.read_until(b'\r', buf).await?;
    if r.read_u8().await? != b'\n' {
        bail!("Expected '\\r\\n'");
    };
    bytes += 1;

    buf.pop().unwrap();
    Ok(bytes)
}

/// Take all the bytes, up to the \r\n and append them to `buf`.  Reads out the \r\n from the
/// reader
async fn take_delim<R>(r: &mut R) -> anyhow::Result<usize>
where
    R: AsyncBufRead + Unpin,
{
    let mut buf = [0u8; 2];
    r.read_exact(&mut buf).await.context("reading delim")?;

    ensure!(buf == *b"\r\n", "Expected buf to be '\\r\\n', got {buf:?}");
    Ok(buf.len())
}

pub async fn get_rdb<R>(r: &mut R) -> anyhow::Result<Vec<u8>>
where
    R: AsyncBufRead + Unpin,
{
    let kind = r.read_u8().await?;
    let kind = DataKind::try_from(kind)?;
    ensure!(
        kind == DataKind::BulkString,
        "Expected rdb to start with '$'"
    );

    let mut buf = Vec::new();
    take_until_delim(r, &mut buf)
        .await
        .context("reading \\r\\n")?;

    let len: usize = String::from_utf8(std::mem::take(&mut buf))
        .context("invalid utf-8 string")?
        .parse()
        .context("invalid length string")?;

    buf.resize(len, 0);

    r.read_exact(&mut buf).await?;

    Ok(buf)
}

pub async fn parse<R>(r: &mut R) -> anyhow::Result<(serde_json::Value, usize)>
where
    R: AsyncBufRead + Unpin,
{
    let mut bytes = 0;
    let kind = r.read_u8().await?;
    bytes += 1;
    let kind = DataKind::try_from(kind)?;

    let mut buf = Vec::new();
    let value = match kind {
        DataKind::SimpleString => {
            bytes += take_until_delim(r, &mut buf).await?;
            serde_json::Value::from(
                String::from_utf8(buf).context("invalid utf-8 in simple string")?,
            )
        }
        DataKind::SimpleError => todo!(),
        DataKind::Integer => todo!(),
        DataKind::BulkString => {
            bytes += take_until_delim(r, &mut buf).await?;

            let len: usize = String::from_utf8(std::mem::take(&mut buf))
                .context("invalid utf-8 string")?
                .parse()
                .context("invalid length string")?;

            buf.resize(len, 0);

            bytes += r.read_exact(&mut buf).await?;

            bytes += take_delim(r).await?;

            // TODO: Confirm that this is a valid assumtion
            let data = String::from_utf8(buf).context("invalid utf-8 string")?;

            serde_json::Value::String(data)
        }
        DataKind::Array => {
            bytes += take_until_delim(r, &mut buf).await?;

            let len: usize = String::from_utf8(buf)
                .context("invalid utf-8 string")?
                .parse()
                .context("invalid length string")?;

            let mut array = Vec::with_capacity(len);

            for i in 0..len {
                let (value, num_bytes) = Box::pin(parse(r))
                    .await
                    .with_context(|| format!("parsing value at index {i} in array"))?;
                bytes += num_bytes;
                array.push(value);
            }

            serde_json::Value::Array(array)
        }
        DataKind::Boolean => todo!(),
        DataKind::Double => todo!(),
        DataKind::BigNumber => todo!(),
        DataKind::BulkError => todo!(),
        DataKind::VerbatimString => todo!(),
        DataKind::Map => todo!(),
        DataKind::Attribute => todo!(),
        DataKind::Set => todo!(),
        DataKind::Push => todo!(),
    };

    Ok((value, bytes))
}

#[derive(Clone, Debug)]
#[repr(u8)]
pub enum Value {
    SimpleString(String),
    SimpleError(String),
    Integer(i64),
    BulkString(String),
    Rdb(Vec<u8>),
    Null,
    Array(Vec<Value>),
    Boolean(bool),
    Double(f64),
    BigNumber(i128),
    BulkError(String),
    VerbatimString { encoding: [u8; 3], data: Vec<u8> },
    Map(HashMap<Value, Value>),
    Attribute(HashMap<Value, Value>),
    Set(HashSet<Value>),
    Push(Vec<Value>),
}

impl Value {
    pub fn bulk_string(arg: impl Into<String>) -> Value {
        Self::BulkString(arg.into())
    }

    pub fn simple_string(arg: impl Into<String>) -> Value {
        Self::SimpleString(arg.into())
    }

    pub fn bulk_error(arg: impl Into<String>) -> Value {
        Self::BulkError(arg.into())
    }

    pub fn simple_error(arg: impl Into<String>) -> Value {
        Self::SimpleError(arg.into())
    }

    pub async fn write_to<W>(&self, w: &mut W) -> anyhow::Result<()>
    where
        W: AsyncWrite + Unpin,
    {
        match self {
            Value::SimpleString(s) => {
                w.write_u8(DataKind::SimpleString.into()).await?;
                w.write_all(format!("{s}\r\n").as_bytes()).await?;
            }
            Value::SimpleError(e) => {
                w.write_u8(DataKind::SimpleError.into()).await?;
                w.write_all(format!("{e}\r\n").as_bytes()).await?;
            }
            Value::Integer(n) => {
                w.write_u8(DataKind::Integer.into()).await?;
                w.write_all(format!("{n}\r\n").as_bytes()).await?;
            }
            Value::BulkString(s) => {
                w.write_u8(DataKind::BulkString.into()).await?;
                w.write_all(format!("{}\r\n", s.len()).as_bytes()).await?;
                w.write_all(s.as_bytes()).await?;
                w.write_all(b"\r\n").await?;
            }
            Value::Rdb(s) => {
                w.write_u8(DataKind::BulkString.into()).await?;
                w.write_all(format!("{}\r\n", s.len()).as_bytes()).await?;
                w.write_all(s).await?;
            }
            Value::Null => w
                .write_all(b"$-1\r\n")
                .await
                .context("writing null json value")?,
            Value::Array(a) => {
                w.write_u8(DataKind::Array.into()).await?;
                w.write_all(format!("{}\r\n", a.len()).as_bytes()).await?;
                for (i, v) in a.iter().enumerate() {
                    Box::pin(v.write_to(w))
                        .await
                        .with_context(|| format!("writing value at index {i} in array"))?;
                }
            }
            Value::Boolean(_) => todo!(),
            Value::Double(_) => todo!(),
            Value::BigNumber(_) => todo!(),
            Value::BulkError(_) => todo!(),
            Value::VerbatimString { .. } => todo!(),
            Value::Map(_) => todo!(),
            Value::Attribute(_) => todo!(),
            Value::Set(_) => todo!(),
            Value::Push(_) => todo!(),
        }

        Ok(())
    }
}

impl Hash for Value {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        match self {
            Value::SimpleString(x) => x.hash(state),
            Value::SimpleError(x) => x.hash(state),
            Value::Integer(x) => x.hash(state),
            Value::BulkString(x) => x.hash(state),
            Value::Rdb(x) => x.hash(state),
            Value::Null => 0.hash(state),
            Value::Array(x) => x.hash(state),
            Value::Boolean(x) => x.hash(state),
            Value::Double(x) => (unsafe { *(x as *const f64 as *const u64) }).hash(state),
            Value::BigNumber(x) => x.hash(state),
            Value::BulkError(x) => x.hash(state),
            Value::VerbatimString { encoding, data } => (encoding, data).hash(state),
            Value::Map(_) => todo!("hash a hash map"),
            Value::Attribute(_) => todo!("hash a hash map"),
            Value::Set(_) => todo!("hash a hash set"),
            Value::Push(x) => x.hash(state),
        }
    }
}

macro_rules! from_int {
    ($($ty: ident),+) => {
        $(
        impl From<$ty> for Value {
            fn from(value: $ty) -> Self {
                Value::Integer(value as i64)
            }
        }
        )+
    }
}

from_int![u8, u16, u32, usize];
from_int![i8, i16, i32, i64, isize];

impl From<Vec<Value>> for Value {
    fn from(value: Vec<Value>) -> Self {
        Value::Array(value)
    }
}

impl From<String> for Value {
    fn from(value: String) -> Self {
        Value::BulkString(value)
    }
}

impl From<&str> for Value {
    fn from(value: &str) -> Self {
        Value::BulkString(value.into())
    }
}

impl From<&String> for Value {
    fn from(value: &String) -> Self {
        Value::BulkString(value.clone())
    }
}

impl<U> FromIterator<U> for Value
where
    U: Into<Value>,
{
    fn from_iter<T: IntoIterator<Item = U>>(iter: T) -> Self {
        Value::Array(iter.into_iter().map(Into::into).collect())
    }
}
