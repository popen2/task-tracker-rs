use futures::FutureExt;
use task_tracker::{TaskResult, TaskTracker};
use tokio::{task::yield_now, time};

#[tokio::test(flavor = "current_thread")]
async fn test_two_tasks() {
    env_logger::init();

    log::info!("Creating tracker");
    let tracker = TaskTracker::<u64, u32>::new();

    log::info!("Adding task 0");
    assert!(matches!(tracker.add(0, async { 0 }.boxed()).await, None));

    log::info!("Overriding task 0");
    assert!(matches!(
        tracker.add(0, async { 0 }.boxed()).await,
        Some(TaskResult::Cancelled)
    ));

    log::info!("Adding task 1");
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

    log::info!("Removing task 1");
    assert!(matches!(
        tracker.remove(&1).await,
        Some(TaskResult::Cancelled)
    ));

    yield_now().await;

    log::info!("Waiting for tasks");
    let results = tracker.wait_for_tasks().await;
    log::debug!("Results: {:?}", results);

    assert!(matches!(results[&0], TaskResult::Done(0)));
}
