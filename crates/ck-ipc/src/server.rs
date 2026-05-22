use crate::protocol::{read_message, write_message, ProtocolError};
use serde::{de::DeserializeOwned, Serialize};
use tokio::net::windows::named_pipe::{NamedPipeServer, ServerOptions};

pub struct PipeServer {
    pipe_name: String,
}

impl PipeServer {
    pub fn new(name: &str) -> Self {
        Self {
            pipe_name: format!(r"\\.\pipe\{}", name),
        }
    }

    pub fn pipe_name(&self) -> &str {
        &self.pipe_name
    }

    /// Create the pipe endpoint (makes it visible to clients) and return a
    /// listener that can be awaited separately from spawning workers.
    pub fn listen(&self) -> Result<PipeListener, ProtocolError> {
        let server = ServerOptions::new()
            .first_pipe_instance(true)
            .create(&self.pipe_name)?;
        Ok(PipeListener { server })
    }
}

/// A created but not yet connected pipe endpoint. Workers can now see and
/// connect to the pipe. Call `accept()` to wait for the first client.
pub struct PipeListener {
    server: NamedPipeServer,
}

impl PipeListener {
    pub async fn accept(self) -> Result<PipeConnection, ProtocolError> {
        self.server.connect().await?;
        Ok(PipeConnection { pipe: self.server })
    }
}

pub struct PipeConnection {
    pipe: NamedPipeServer,
}

impl PipeConnection {
    pub async fn read<T: DeserializeOwned>(&mut self) -> Result<T, ProtocolError> {
        read_message(&mut self.pipe).await
    }

    pub async fn write<T: Serialize>(&mut self, msg: &T) -> Result<(), ProtocolError> {
        write_message(&mut self.pipe, msg).await
    }
}
