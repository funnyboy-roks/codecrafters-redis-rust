use anyhow::{bail, ensure, Context};
use serde_json::Value;
use tokio::io::{AsyncBufRead, AsyncBufReadExt, AsyncReadExt, AsyncWrite, AsyncWriteExt};

#[derive(Copy, Clone, Debug)]
#[repr(u8)]
pub enum DataKind {
    SimpleString = b'+',
    SimpleError = b'-',
    Integer = b':',
    BulkString = b'$',
    Array = b'*',
    Null = b'_',
    Boolean = b'#',
    Double = b',',
    BugNumber = b'(',
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
            // b'-' => Ok(Self::SimpleError),
            // b':' => Ok(Self::Integer),
            b'$' => Ok(Self::BulkString),
            b'*' => Ok(Self::Array),
            // b'_' => Ok(Self::Null),
            // b'#' => Ok(Self::Boolean),
            // b',' => Ok(Self::Double),
            // b'(' => Ok(Self::BugNumber),
            // b'!' => Ok(Self::BulkError),
            // b'=' => Ok(Self::VerbatimString),
            // b'%' => Ok(Self::Map),
            // b'|' => Ok(Self::Attribute),
            // b'~' => Ok(Self::Set),
            // b'>' => Ok(Self::Push),
            _ => bail!("Unknown datakind symbol: '{}'", value as char),
        }
    }
}

/// Take all the bytes, up to the \r\n and append them to `buf`.  Reads out the \r\n from the
/// reader
async fn take_until_delim<R>(r: &mut R, buf: &mut Vec<u8>) -> anyhow::Result<()>
where
    R: AsyncBufRead + Unpin,
{
    r.read_until(b'\r', buf).await?;
    if r.read_u8().await? != b'\n' {
        bail!("Expected '\\r\\n'");
    };

    buf.pop().unwrap();
    Ok(())
}

/// Take all the bytes, up to the \r\n and append them to `buf`.  Reads out the \r\n from the
/// reader
async fn take_delim<R>(r: &mut R) -> anyhow::Result<()>
where
    R: AsyncBufRead + Unpin,
{
    let mut buf = [0u8; 2];
    r.read_exact(&mut buf).await.context("reading delim")?;

    ensure!(buf == *b"\r\n", "Expected buf to be '\\r\\n', got {buf:?}");
    Ok(())
}

pub async fn parse<R>(r: &mut R) -> anyhow::Result<serde_json::Value>
where
    R: AsyncBufRead + Unpin,
{
    let kind = r.read_u8().await?;
    let kind = DataKind::try_from(kind)?;

    let mut buf = Vec::new();
    let value = match kind {
        DataKind::SimpleString => todo!(),
        DataKind::SimpleError => todo!(),
        DataKind::Integer => todo!(),
        DataKind::BulkString => {
            take_until_delim(r, &mut buf).await?;

            let len: usize = String::from_utf8(std::mem::take(&mut buf))
                .context("invalid utf-8 string")?
                .parse()
                .context("invalid length string")?;

            buf.resize(len, 0);

            r.read_exact(&mut buf).await?;

            take_delim(r).await?;

            // TODO: Confirm that this is a valid assumtion
            let data = String::from_utf8(buf).context("invalid utf-8 string")?;

            Value::String(data)
        }
        DataKind::Array => {
            take_until_delim(r, &mut buf).await?;

            let len: usize = String::from_utf8(buf)
                .context("invalid utf-8 string")?
                .parse()
                .context("invalid length string")?;

            let mut array = Vec::with_capacity(len);

            for i in 0..len {
                let value = Box::pin(parse(r))
                    .await
                    .with_context(|| format!("parsing value at index {i} in array"))?;
                array.push(value);
            }

            serde_json::Value::Array(array)
        }
        DataKind::Null => todo!(),
        DataKind::Boolean => todo!(),
        DataKind::Double => todo!(),
        DataKind::BugNumber => todo!(),
        DataKind::BulkError => todo!(),
        DataKind::VerbatimString => todo!(),
        DataKind::Map => todo!(),
        DataKind::Attribute => todo!(),
        DataKind::Set => todo!(),
        DataKind::Push => todo!(),
    };

    Ok(value)
}

pub async fn write<W>(w: &mut W, value: serde_json::Value) -> anyhow::Result<()>
where
    W: AsyncWrite + Unpin,
{
    match value {
        Value::Null => todo!(),
        Value::Bool(_) => todo!(),
        Value::Number(_) => todo!(),
        Value::String(s) => {
            w.write_u8(DataKind::BulkString.into()).await?;
            w.write_all(format!("{}\r\n", s.len()).as_bytes()).await?;
            w.write_all(s.as_bytes()).await?;
            w.write_all(b"\r\n").await?;
        }
        Value::Array(a) => {
            w.write_u8(DataKind::Array.into()).await?;
            w.write_all(format!("{}\r\n", a.len()).as_bytes()).await?;
            for (i, v) in a.into_iter().enumerate() {
                Box::pin(write(w, v))
                    .await
                    .with_context(|| format!("writing value at index {i} in array"))?;
            }
            w.write_all(b"\r\n").await?;
        }
        Value::Object(_) => todo!(),
    }

    Ok(())
}
