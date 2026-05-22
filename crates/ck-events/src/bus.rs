use tokio::sync::broadcast;
use crate::types::KernelEvent;

#[derive(Clone)]
pub struct EventBus {
    tx: broadcast::Sender<KernelEvent>,
}

impl EventBus {
    pub fn new(capacity: usize) -> Self {
        let (tx, _) = broadcast::channel(capacity);
        Self { tx }
    }

    pub fn emit(&self, event: KernelEvent) {
        let _ = self.tx.send(event);
    }

    pub fn subscribe(&self) -> broadcast::Receiver<KernelEvent> {
        self.tx.subscribe()
    }
}
