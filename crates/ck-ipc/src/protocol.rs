use serde::{de::DeserializeOwned, Serialize};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

#[derive(Debug, thiserror::Error)]
pub enum ProtocolError {
    #[error("serialization error: {0}")]
    Serialize(#[from] rmp_serde::encode::Error),
    #[error("deserialization error: {0}")]
    Deserialize(#[from] rmp_serde::decode::Error),
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),
    #[error("incomplete message: expected {expected} bytes, got {got}")]
    Incomplete { expected: usize, got: usize },
}

/// Encode a message with 4-byte big-endian length prefix + msgpack payload.
pub fn encode_message<T: Serialize>(msg: &T) -> Result<Vec<u8>, ProtocolError> {
    let payload = rmp_serde::to_vec(msg)?;
    let len = payload.len() as u32;
    let mut buf = Vec::with_capacity(4 + payload.len());
    buf.extend_from_slice(&len.to_be_bytes());
    buf.extend_from_slice(&payload);
    Ok(buf)
}

/// Decode a framed buffer (4-byte length prefix + msgpack payload).
pub fn decode_message<T: DeserializeOwned>(data: &[u8]) -> Result<T, ProtocolError> {
    if data.len() < 4 {
        return Err(ProtocolError::Incomplete { expected: 4, got: data.len() });
    }
    let len = u32::from_be_bytes([data[0], data[1], data[2], data[3]]) as usize;
    let payload = &data[4..];
    if payload.len() < len {
        return Err(ProtocolError::Incomplete { expected: len, got: payload.len() });
    }
    Ok(rmp_serde::from_slice(&payload[..len])?)
}

/// Read a length-prefixed msgpack message from an async reader.
pub async fn read_message<T: DeserializeOwned>(
    reader: &mut (impl AsyncReadExt + Unpin),
) -> Result<T, ProtocolError> {
    let mut len_buf = [0u8; 4];
    reader.read_exact(&mut len_buf).await?;
    let len = u32::from_be_bytes(len_buf) as usize;
    let mut payload = vec![0u8; len];
    reader.read_exact(&mut payload).await?;
    Ok(rmp_serde::from_slice(&payload)?)
}

/// Write a length-prefixed msgpack message to an async writer.
pub async fn write_message<T: Serialize>(
    writer: &mut (impl AsyncWriteExt + Unpin),
    msg: &T,
) -> Result<(), ProtocolError> {
    let payload = rmp_serde::to_vec(msg)?;
    let len = (payload.len() as u32).to_be_bytes();
    writer.write_all(&len).await?;
    writer.write_all(&payload).await?;
    Ok(())
}
