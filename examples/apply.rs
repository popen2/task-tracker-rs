use maplit::hashmap;
use std::time::Duration;
use task_tracker::TaskTracker;
use tokio::time::sleep;
use tracing::{info, metadata::LevelFilter};

#[tokio::main(flavor = "current_thread")]
async fn main() {
    tracing_subscriber::fmt()
        .with_max_level(LevelFilter::TRACE)
        .with_target(true)
        .with_ansi(true)
        .with_level(true)
        .init();

    let tracker = TaskTracker::new();

    info!("Creating initial tasks");
    let finished = tracker
        .apply(
            hashmap! {
                "one" => 1,
                "two" => 2,
                "three" => 3,
                "four" => 4,
            },
            |(key, _)| key,
            |(_, value)| async move {
                sleep(Duration::from_secs(value)).await;
            },
        )
        .await;
    assert!(finished.is_empty());

    sleep(Duration::from_millis(10)).await;

    info!("Checking for finished tasks (there shouldn't be any)");
    let finished = tracker
        .apply(
            hashmap! {
                "one" => 1,
                "two" => 2,
                "three" => 3,
                "four" => 4,
            },
            |(key, _)| key,
            |(_, value)| async move {
                sleep(Duration::from_secs(value)).await;
            },
        )
        .await;
    info!(finished.len = finished.len());
    assert!(finished.is_empty());

    info!("Sleeping for 2s");
    sleep(Duration::from_secs(2)).await;

    info!("Ensuring tasks again");
    let finished = tracker
        .apply(
            hashmap! {
                "one" => 1,
                "two" => 2,
                "three" => 3,
                "four" => 4,
            },
            |(key, _)| key,
            |(_, value)| async move {
                sleep(Duration::from_secs(value)).await;
            },
        )
        .await;
    info!(finished.len = finished.len());
    assert_eq!(finished.len(), 2);

    info!("Waiting for all tasks");
    let finished = tracker.wait_for_tasks().await;
    info!(finished.len = finished.len());
    assert_eq!(finished.len(), 4);
}
