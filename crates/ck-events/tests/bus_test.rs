use ck_events::bus::EventBus;
use ck_events::types::KernelEvent;

#[tokio::test]
async fn test_emit_and_receive() {
    let bus = EventBus::new(128);
    let mut rx = bus.subscribe();
    let event = KernelEvent::TaskCreated {
        task_id: "t1".into(),
        goal: "test goal".into(),
        timestamp: 1000,
    };
    bus.emit(event);
    let received = rx.recv().await.unwrap();
    match received {
        KernelEvent::TaskCreated { task_id, .. } => assert_eq!(task_id, "t1"),
        _ => panic!("wrong event type"),
    }
}

#[tokio::test]
async fn test_multiple_subscribers() {
    let bus = EventBus::new(128);
    let mut rx1 = bus.subscribe();
    let mut rx2 = bus.subscribe();
    bus.emit(KernelEvent::TaskCompleted { task_id: "t1".into(), duration_ms: 100, steps_executed: 3 });
    assert!(rx1.recv().await.is_ok());
    assert!(rx2.recv().await.is_ok());
}
