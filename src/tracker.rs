use crate::inner_task::{InnerTask, TaskResult};
use std::collections::HashMap;
use std::collections::HashSet;
use std::hash::Hash;
use std::sync::Arc;
use tokio::sync::RwLock;
use tracing::debug;

#[derive(Clone)]
pub struct TaskTracker<K, R>
where
    K: Clone + Eq + Hash + Sync + Send + std::fmt::Debug + 'static,
    R: Sync + Send + std::fmt::Debug + 'static,
{
    inner: Arc<RwLock<HashMap<K, InnerTask<K, R>>>>,
}

impl<K, R> TaskTracker<K, R>
where
    K: Clone + Eq + Hash + Sync + Send + std::fmt::Debug + 'static,
    R: Sync + Send + std::fmt::Debug + 'static,
{
    pub fn new() -> Self {
        Self {
            inner: Default::default(),
        }
    }

    pub async fn add<Fut>(&self, key: K, fut: Fut) -> Option<TaskResult<R>>
    where
        Fut: std::future::Future<Output = R> + Send + 'static,
    {
        let prev_result = self.remove(&key).await;
        self.inner
            .write()
            .await
            .insert(key.clone(), InnerTask::new(key, fut));
        prev_result
    }

    pub async fn remove(&self, key: &K) -> Option<TaskResult<R>> {
        let prev_task = self.inner.write().await.remove(key);
        if let Some(prev_task) = prev_task {
            debug!(?key, %prev_task.id, "removed previous task");
            Some(prev_task.cancel_and_wait().await)
        } else {
            None
        }
    }

    pub async fn apply<S, F, Fut>(&self, keys: S, create_task: F)
    where
        S: Iterator<Item = K>,
        F: Fn(&K) -> Fut,
        Fut: std::future::Future<Output = R> + Send + 'static,
    {
        let old_keys: HashSet<_> = self.inner.read().await.keys().cloned().collect();
        let new_keys: HashSet<_> = keys.collect();

        for to_remove in old_keys.difference(&new_keys) {
            self.remove(to_remove).await;
        }

        for to_add in new_keys.difference(&old_keys) {
            self.add(to_add.to_owned(), create_task(to_add)).await;
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
