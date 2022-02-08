use futures::future::BoxFuture;
use log::*;
use std::collections::HashMap;
use std::collections::HashSet;
use std::fmt::Debug;
use std::hash::Hash;
use std::sync::Arc;
use tokio::sync::oneshot;
use tokio::sync::{RwLock, RwLockWriteGuard};
use tokio::{select, task};

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
        debug!("Adding new task for key={:?}", key);
        let join_handle = {
            let mut inner = self.inner.write().await;
            self._add(&mut inner, key, task)
        };
        Self::_await_join_handle(join_handle).await
    }

    fn _add<'a>(
        &'a self,
        inner: &mut RwLockWriteGuard<'a, Inner<K, R>>,
        key: K,
        task: BoxFuture<'static, R>,
    ) -> Option<task::JoinHandle<TaskResult<R>>> {
        let (stop_tx, stop_rx) = oneshot::channel::<()>();
        let join_handle = Self::_remove(inner, &key);
        trace!("Adding new stop_ch for {:?}", key);
        inner.stop_chs.insert(key.clone(), stop_tx);
        trace!("Adding new join handle for {:?}", key);
        let inner2 = self.inner.clone();
        inner.tasks.insert(
            key.clone(),
            task::spawn(async move {
                debug!("Task {:?} started", key);
                let result = select! {
                    task_result = task => {
                        debug!("Task {:?} finished: {:?}", key, task_result);
                        TaskResult::Done(task_result)
                    },
                    _ = stop_rx => {
                        debug!("Task {:?} cancelled", key);
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
        debug!("Removing task with key={:?}", key);
        let join_handle = {
            let mut inner = self.inner.write().await;
            Self::_remove(&mut inner, key)
        };
        Self::_await_join_handle(join_handle).await
    }

    async fn _await_join_handle(
        handle: Option<task::JoinHandle<TaskResult<R>>>,
    ) -> Option<TaskResult<R>> {
        if let Some(handle) = handle {
            Some(match handle.await {
                Ok(task_result) => task_result,
                Err(join_error) => TaskResult::JoinError(join_error),
            })
        } else {
            None
        }
    }

    fn _remove(
        inner: &mut RwLockWriteGuard<'_, Inner<K, R>>,
        key: &K,
    ) -> Option<task::JoinHandle<TaskResult<R>>> {
        trace!("Removing stop_ch of {:?}", key);
        if let Some(stop_tx) = inner.stop_chs.remove(key) {
            stop_tx.send(()).unwrap();
        }
        trace!("Removing join handle of {:?}", key);
        if let Some(join_handle) = inner.tasks.remove(key) {
            trace!("Found join handle for task {:?}", key);
            Some(join_handle)
        } else {
            trace!("There was no join handle for {:?}", key);
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
            Self::_remove(&mut inner, to_remove);
        }

        for to_add in new_keys.difference(&old_keys) {
            self._add(&mut inner, to_add.to_owned(), create_task(to_add));
        }
    }

    pub async fn wait_for_tasks(&self) -> HashMap<K, TaskResult<R>> {
        let mut results = HashMap::new();
        debug!("Waiting for all tasks to finish");
        let mut inner = self.inner.write().await;
        for (key, join_handle) in inner.tasks.drain() {
            debug!("Waiting for task {:?}", key);
            let result = Self::_await_join_handle(Some(join_handle)).await.unwrap();
            results.insert(key, result);
        }
        inner.stop_chs.drain();
        results
    }
}
