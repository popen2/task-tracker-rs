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

pub struct Inner<K, R>
where
    K: Clone + Eq + Hash + Sync + Send + Debug + 'static,
    R: Sync + Send + std::fmt::Debug + 'static,
{
    stop_chs: HashMap<K, oneshot::Sender<()>>,
    tasks: HashMap<K, task::JoinHandle<TaskResult<R>>>,
}

impl<K, R> TaskTracker<K, R>
where
    K: Clone + Eq + Hash + Sync + Send + Debug + 'static,
    R: Sync + Send + std::fmt::Debug + 'static,
{
    pub fn new() -> Self {
        Self {
            inner: Arc::new(RwLock::new(Inner {
                stop_chs: Default::default(),
                tasks: Default::default(),
            })),
        }
    }

    pub async fn add(&self, key: K, task: BoxFuture<'static, R>) -> Option<TaskResult<R>> {
        let join_handle = {
            let mut inner = self.inner.write().await;
            self.add_inner(&mut inner, key.clone(), task)
        };
        Self::await_join_handle(&key, join_handle).await
    }

    fn add_inner<'a>(
        &'a self,
        inner: &mut RwLockWriteGuard<'a, Inner<K, R>>,
        key: K,
        task: BoxFuture<'static, R>,
    ) -> Option<task::JoinHandle<TaskResult<R>>> {
        let (stop_tx, stop_rx) = oneshot::channel::<()>();
        let join_handle = Self::remove_inner(inner, &key);
        debug!(?key, "adding new task");
        inner.stop_chs.insert(key.clone(), stop_tx);
        let inner2 = self.inner.clone();
        inner.tasks.insert(
            key.clone(),
            task::spawn(async move {
                debug!(?key, "task started");
                let result = select! {
                    task_result = task => {
                        debug!(?key, ?task_result,"task finished");
                        TaskResult::Done(task_result)
                    },
                    _ = stop_rx => {
                        debug!(?key, "task cancelled");
                        TaskResult::Cancelled
                    },
                };
                inner2.write().await.stop_chs.remove(&key);
                result
            }),
        );
        join_handle
    }

    pub async fn remove(&self, key: &K) -> Option<TaskResult<R>> {
        let join_handle = {
            let mut inner = self.inner.write().await;
            Self::remove_inner(&mut inner, key)
        };
        Self::await_join_handle(key, join_handle).await
    }

    async fn await_join_handle(
        key: &K,
        handle: Option<task::JoinHandle<TaskResult<R>>>,
    ) -> Option<TaskResult<R>> {
        if let Some(handle) = handle {
            debug!(?key, "waiting for task to finish");
            Some(match handle.await {
                Ok(task_result) => task_result,
                Err(join_error) => TaskResult::JoinError(join_error),
            })
        } else {
            None
        }
    }

    fn remove_inner(
        inner: &mut RwLockWriteGuard<'_, Inner<K, R>>,
        key: &K,
    ) -> Option<task::JoinHandle<TaskResult<R>>> {
        if let Some(stop_tx) = inner.stop_chs.remove(key) {
            stop_tx.send(()).unwrap();
        }
        if let Some(join_handle) = inner.tasks.remove(key) {
            debug!(?key, "removed previous task");
            Some(join_handle)
        } else {
            None
        }
    }

    pub async fn apply<S, F>(&self, keys: S, create_task: F)
    where
        S: Iterator<Item = K>,
        F: Fn(&K) -> BoxFuture<'static, R>,
    {
        let mut inner = self.inner.write().await;
        let old_keys: HashSet<_> = inner.stop_chs.keys().cloned().collect();
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
        for (key, join_handle) in inner.tasks.drain() {
            let result = Self::await_join_handle(&key, Some(join_handle))
                .await
                .unwrap();
            results.insert(key, result);
        }
        inner.stop_chs.drain();
        results
    }
}
