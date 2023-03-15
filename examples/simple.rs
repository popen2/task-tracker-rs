use futures::{future::pending, FutureExt};
use task_tracker::{TaskResult, TaskTracker};
use tokio::{task::yield_now, time};
use tracing::{info, metadata::LevelFilter};

#[tokio::main(flavor = "current_thread")]
async fn main() {
    tracing_subscriber::fmt()
        .with_max_level(LevelFilter::TRACE)
        .with_target(true)
        .with_ansi(true)
        .with_level(true)
        .init();

    info!("Creating tracker");
    let tracker = TaskTracker::<u64, u32>::new();

    info!("Adding task 0");
    assert!(matches!(
        tracker.add(0, async { pending().await }.boxed()).await,
        None
    ));

    yield_now().await;

    info!("Overriding task 0");
    assert!(matches!(
        tracker.add(0, async { 0 }.boxed()).await,
        Some(TaskResult::Cancelled)
    ));

    yield_now().await;

    info!("Adding task 1");
    assert!(matches!(
        tracker
            .add(
                1,
                async {
                    time::sleep(time::Duration::from_secs(10000)).await;
                    1
                }
                .boxed(),
            )
            .await,
        None
    ));

    yield_now().await;

    info!("Removing task 1");
    assert!(matches!(
        tracker.remove(&1).await,
        Some(TaskResult::Cancelled)
    ));

    yield_now().await;

    info!("Waiting for tasks");
    let results = tracker.wait_for_tasks().await;
    info!(?results);

    assert!(matches!(results[&0], TaskResult::Done(0)));
}
