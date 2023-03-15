use futures::future::BoxFuture;
use std::collections::HashMap;
use std::collections::HashSet;
use std::fmt::Debug;
use std::hash::Hash;
use std::sync::Arc;
use tokio::sync::oneshot;
use tokio::sync::{RwLock, RwLockWriteGuard};
use tokio::{select, task};
use tracing::debug;
use uuid::Uuid;

#[derive(Clone)]
pub struct TaskTracker<K, R>
where
    K: Clone + Eq + Hash + Sync + Send + Debug + 'static,
    R: Sync + Send + std::fmt::Debug + 'static,
{
    inner: Arc<RwLock<Inner<K, R>>>,
}

#[derive(Debug)]
pub enum TaskResult<R>
where
    R: Sync + Send + std::fmt::Debug + 'static,
{
    Done(R),
    Cancelled,
    JoinError(task::JoinError),
}

pub struct InnerTask<R>
where
    R: Sync + Send + std::fmt::Debug + 'static,
{
    id: Uuid,
    stop_tx: oneshot::Sender<()>,
    join_handle: task::JoinHandle<TaskResult<R>>,
}

impl<R> InnerTask<R>
where
    R: Sync + Send + std::fmt::Debug + 'static,
{
    async fn cancel_and_wait(self) -> TaskResult<R> {
        debug!(task_id = %self.id, "waiting for task to finish");
        self.stop_tx.send(()).unwrap();
        match self.join_handle.await {
            Ok(task_result) => task_result,
            Err(join_error) => TaskResult::JoinError(join_error),
        }
    }

    async fn wait(self) -> TaskResult<R> {
        match self.join_handle.await {
            Ok(task_result) => task_result,
            Err(join_error) => TaskResult::JoinError(join_error),
        }
    }
}

pub type Inner<K, R> = HashMap<K, InnerTask<R>>;

impl<K, R> TaskTracker<K, R>
where
    K: Clone + Eq + Hash + Sync + Send + Debug + 'static,
    R: Sync + Send + std::fmt::Debug + 'static,
{
    pub fn new() -> Self {
        Self {
            inner: Default::default(),
        }
    }

    pub async fn add(&self, key: K, task: BoxFuture<'static, R>) -> Option<TaskResult<R>> {
        let prev_task = {
            let mut inner = self.inner.write().await;
            self.add_inner(&mut inner, key.clone(), task)
        };
        if let Some(prev_task) = prev_task {
            Some(prev_task.cancel_and_wait().await)
        } else {
            None
        }
    }

    fn add_inner<'a>(
        &'a self,
        inner: &mut RwLockWriteGuard<'a, Inner<K, R>>,
        key: K,
        task: BoxFuture<'static, R>,
    ) -> Option<InnerTask<R>> {
        let task_id = Uuid::new_v4();
        let prev_task = Self::remove_inner(inner, &key);
        let (stop_tx, stop_rx) = oneshot::channel::<()>();
        debug!(?key, %task_id, "adding new task");
        inner.insert(
            key.clone(),
            InnerTask {
                id: task_id,
                stop_tx,
                join_handle: task::spawn(async move {
                    debug!(?key, %task_id, "task started");
                    let result = select! {
                        task_result = task => {
                            debug!(?key, %task_id, ?task_result, "task finished");
                            TaskResult::Done(task_result)
                        },
                        _ = stop_rx => {
                            debug!(?key, %task_id, "task cancelled");
                            TaskResult::Cancelled
                        },
                    };
                    result
                }),
            },
        );
        prev_task
    }

    pub async fn remove(&self, key: &K) -> Option<TaskResult<R>> {
        let prev_task = {
            let mut inner = self.inner.write().await;
            Self::remove_inner(&mut inner, key)
        };
        if let Some(prev_task) = prev_task {
            Some(prev_task.cancel_and_wait().await)
        } else {
            None
        }
    }

    fn remove_inner(
        inner: &mut RwLockWriteGuard<'_, Inner<K, R>>,
        key: &K,
    ) -> Option<InnerTask<R>> {
        inner.remove(key).map(|inner_task| {
            debug!(?key, %inner_task.id, "removed previous task");
            inner_task
        })
    }

    pub async fn apply<S, F>(&self, keys: S, create_task: F)
    where
        S: Iterator<Item = K>,
        F: Fn(&K) -> BoxFuture<'static, R>,
    {
        let mut inner = self.inner.write().await;
        let old_keys: HashSet<_> = inner.keys().cloned().collect();
        let new_keys: HashSet<_> = keys.collect();

        for to_remove in old_keys.difference(&new_keys) {
            Self::remove_inner(&mut inner, to_remove);
        }

        for to_add in new_keys.difference(&old_keys) {
            self.add_inner(&mut inner, to_add.to_owned(), create_task(to_add));
        }
    }

    pub async fn wait_for_tasks(&self) -> HashMap<K, TaskResult<R>> {
        let mut results = HashMap::new();
        debug!("waiting for all tasks to finish");
        let mut inner = self.inner.write().await;
        for (key, inner_task) in inner.drain() {
            let result = inner_task.wait().await;
            results.insert(key, result);
        }
        results
    }
}
