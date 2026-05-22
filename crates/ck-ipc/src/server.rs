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

    pub async fn accept(&self) -> Result<PipeConnection, ProtocolError> {
        let server = ServerOptions::new()
            .first_pipe_instance(false)
            .create(&self.pipe_name)?;
        server.connect().await?;
        Ok(PipeConnection { pipe: server })
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
