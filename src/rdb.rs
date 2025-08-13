use std::time::{Duration, SystemTime};

use anyhow::{bail, ensure, Context};
use tokio::{
    io::{AsyncBufRead, AsyncReadExt},
    time::Instant,
};

use crate::{MapValueContent, State};

#[derive(Debug, Clone, Copy)]
pub enum DecodedValue<'a> {
    Bytes(&'a [u8]),
    String(&'a str),
    I8(i8),
    I16(i16),
    I32(i32),
}

impl DecodedValue<'_> {
    fn unwrap_string(&self) -> anyhow::Result<&str> {
        match self {
            DecodedValue::Bytes(_) => bail!("Got bytes, expected string"),
            DecodedValue::String(s) => Ok(s),
            DecodedValue::I8(_) => bail!("Got i8, expected string"),
            DecodedValue::I16(_) => bail!("Got i16, expected string"),
            DecodedValue::I32(_) => bail!("Got i32, expected string"),
        }
    }
}

async fn read_string_encoded<R>(mut r: R, buf: &mut Vec<u8>) -> anyhow::Result<DecodedValue<'_>>
where
    R: AsyncBufRead + Unpin,
{
    let len = r.read_u8().await.context("reading length of string")?;
    let kind = len >> 6;
    let bottom_bits = len & 0b0011_1111;
    let len = match kind {
        0b00 => bottom_bits as usize,
        0b01 => u16::from_le_bytes([
            bottom_bits,
            r.read_u8().await.context("reading second byte of length")?,
        ]) as usize,
        0b10 => r.read_u32_le().await.context("reading length")? as usize,
        0b11 => match bottom_bits {
            0 => {
                return Ok(DecodedValue::I8(
                    r.read_i8().await.context("reading 8-bit number")?,
                ));
            }
            1 => {
                return Ok(DecodedValue::I16(
                    r.read_i16_le().await.context("reading 16-bit number")?,
                ));
            }
            2 => {
                return Ok(DecodedValue::I32(
                    r.read_i32_le().await.context("reading 32-bit number")?,
                ));
            }
            _ => bail!("Unknown special encoding of string: {}", bottom_bits),
        },
        _ => unreachable!("kind = len >> 6, so len is only two bits"),
    };

    buf.resize(len, 0);

    r.read_exact(buf)
        .await
        .with_context(|| format!("reading string of length {len}"))?;

    if let Ok(s) = str::from_utf8(buf) {
        Ok(DecodedValue::String(s))
    } else {
        Ok(DecodedValue::Bytes(buf))
    }
}

async fn read_kv_pair<R>(mut r: R, buf: &mut Vec<u8>) -> anyhow::Result<(String, DecodedValue<'_>)>
where
    R: AsyncBufRead + Unpin,
{
    let key = read_string_encoded(&mut r, buf)
        .await
        .context("reading metadata key")?;

    let DecodedValue::String(key) = key else {
        bail!("key must be a string");
    };
    let key = key.to_string();

    let value = read_string_encoded(&mut r, buf)
        .await
        .context("reading metadata key")?;

    Ok((key, value))
}

pub async fn read<R>(mut r: R, state: &mut State) -> anyhow::Result<()>
where
    R: AsyncBufRead + Unpin,
{
    let mut buf = [0u8; 9];
    r.read_exact(&mut buf)
        .await
        .context("reading magic string and version number")?;

    ensure!(
        buf == *b"REDIS0011",
        "Magic string/version number did not match expected value of 'REDIS0011': {buf:02x?}"
    );

    let mut buf = Vec::new();
    loop {
        let section = r.read_u8().await.context("reading subsection tag")?;
        match section {
            0xfa => {
                eprintln!("---------- metadata subsection ----------");
                // metadata subsection
                let (key, value) = read_kv_pair(&mut r, &mut buf)
                    .await
                    .context("reading metadata key-value pair")?;

                std::eprintln!(
                    "[{}:{}:{}] (key, value) = {:?}",
                    std::file!(),
                    std::line!(),
                    std::column!(),
                    (key, value)
                );
            }
            0xfe => {
                eprintln!("---------- database subsection ----------");
                // database subsection
                let index = r.read_u8().await.context("reading database index")?;
                dbg!(index);

                let fb = r
                    .read_u8()
                    .await
                    .context("reading hash table information")?;
                ensure!(
                    fb == 0xfb,
                    "expected 0xfb for start of hash table information, got 0x{fb:02x}"
                );

                let hash_table_size = r.read_u8().await.context("reading hash table size")?;
                let hash_table_expiry_size =
                    r.read_u8().await.context("reading hash table size")?;

                dbg!(hash_table_size);
                dbg!(hash_table_expiry_size);

                for _ in 0..hash_table_size {
                    let flag = r.read_u8().await.context("reading kv-pair flag")?;

                    let (key, value, expire) = match flag {
                        0x00 => {
                            let (key, value) = read_kv_pair(&mut r, &mut buf)
                                .await
                                .context("reading key-value pair")?;

                            let value = value.unwrap_string()?.to_string();
                            (key, value, None)
                        }
                        0xfd => {
                            // expiry in secs
                            let timestamp =
                                r.read_u32_le().await.context("reading timeout timestamp")?;
                            let expire =
                                SystemTime::UNIX_EPOCH + Duration::from_secs(timestamp.into());

                            let zero = r.read_u8().await.context("reading kv-pair flag")?;
                            ensure!(zero == 0, "kv-pair flag was not zero: 0x{zero:02x}");

                            let (key, value) = read_kv_pair(&mut r, &mut buf)
                                .await
                                .context("reading key-value pair")?;

                            let value = value.unwrap_string()?.to_string();
                            (key, value, Some(expire))
                        }
                        0xfc => {
                            // expiry in millis
                            let timestamp =
                                r.read_u64_le().await.context("reading timeout timestamp")?;
                            let expire = SystemTime::UNIX_EPOCH + Duration::from_secs(timestamp);

                            let zero = r.read_u8().await.context("reading kv-pair flag")?;
                            ensure!(zero == 0, "kv-pair flag was not zero: 0x{zero:02x}");

                            let (key, value) = read_kv_pair(&mut r, &mut buf)
                                .await
                                .context("reading key-value pair")?;

                            let value = value.unwrap_string()?.to_string();
                            (key, value, Some(expire))
                        }
                        _ => bail!("Uknown kv-pair flag: {flag:02x}"),
                    };

                    std::eprintln!(
                        "[{}:{}:{}] db entry (key, value, expire) = {:?}",
                        std::file!(),
                        std::line!(),
                        std::column!(),
                        (&key, &value, &expire)
                    );

                    state.map.insert(
                        key,
                        crate::MapValue {
                            value: MapValueContent::String(value),
                            expires_at: expire.map(|_| Instant::now()),
                        },
                    );
                }
            }
            0xff => {
                eprintln!("---------- eof subsection ----------");
                // EOF subsection
                break;
            }
            tag => {
                bail!("unexpected subsection tag: {tag:02x}");
            }
        }
    }

    Ok(())
}
