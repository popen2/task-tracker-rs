use tokio::select;
use tokio::sync::oneshot;
use tokio::task::{JoinError, JoinHandle};
use tracing::debug;
use uuid::Uuid;

pub struct InnerTask<K, R>
where
    K: Clone + Send + std::fmt::Debug + 'static,
    R: Send + 'static,
{
    pub id: Uuid,
    pub key: K,
    stop_tx: oneshot::Sender<()>,
    join_handle: JoinHandle<TaskResult<R>>,
}

impl<K, R> InnerTask<K, R>
where
    K: Clone + Send + std::fmt::Debug + 'static,
    R: Send + 'static,
{
    pub fn new<Fut>(key: K, fut: Fut) -> Self
    where
        Fut: std::future::Future<Output = R> + Send + 'static,
    {
        let id = Uuid::new_v4();
        let (stop_tx, stop_rx) = oneshot::channel::<()>();
        debug!(?key, %id, "creating new task");
        let key_ = key.clone();
        let fut = fut;
        let task = async move {
            debug!(key = ?key_, task_id = %id, "task started");
            let result = select! {
                task_result = fut => {
                    debug!(key = ?key_, task_id = %id, "task finished");
                    TaskResult::Done(task_result)
                },
                _ = stop_rx => {
                    debug!(key = ?key_, task_id = %id, "task cancelled");
                    TaskResult::Cancelled
                },
            };
            result
        };
        Self {
            id,
            key,
            stop_tx,
            join_handle: tokio::task::spawn(task),
        }
    }

    pub async fn cancel_and_wait(self) -> TaskResult<R> {
        debug!(key = ?self.key, task_id = %self.id, "waiting for task to finish");
        self.stop_tx.send(()).unwrap();
        match self.join_handle.await {
            Ok(task_result) => task_result,
            Err(join_error) => TaskResult::JoinError(join_error),
        }
    }

    pub async fn wait(self) -> TaskResult<R> {
        match self.join_handle.await {
            Ok(task_result) => task_result,
            Err(join_error) => TaskResult::JoinError(join_error),
        }
    }
}

pub enum TaskResult<R>
where
    R: Send,
{
    Done(R),
    Cancelled,
    JoinError(JoinError),
}

impl<R> std::fmt::Debug for TaskResult<R>
where
    R: Send,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{}",
            match self {
                Self::Done(_) => "Done(..)",
                Self::Cancelled => "Cancelled",
                Self::JoinError(_) => "JoinError",
            }
        )
    }
}
