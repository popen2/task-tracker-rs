use crate::inner_task::{InnerTask, TaskResult};
use hashbrown::{HashMap, HashSet};
use std::sync::Arc;
use tokio::sync::RwLock;
use tracing::debug;

#[derive(Clone)]
pub struct TaskTracker<K, R>
where
    K: Clone + Eq + std::hash::Hash + Sync + Send + std::fmt::Debug + 'static,
    R: Sync + Send + std::fmt::Debug + 'static,
{
    inner: Arc<RwLock<HashMap<K, InnerTask<K, R>>>>,
}

impl<K, R> TaskTracker<K, R>
where
    K: Clone + Eq + std::hash::Hash + Sync + Send + std::fmt::Debug + 'static,
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

    pub async fn apply<I, IT, G, F, Fut>(
        &self,
        iter: IT,
        get_key: G,
        create_task: F,
    ) -> HashMap<K, TaskResult<R>>
    where
        IT: IntoIterator<Item = I>,
        G: Fn(&I) -> &K,
        F: Fn(I) -> Fut,
        Fut: std::future::Future<Output = R> + Send + 'static,
    {
        let mut finished = self.remove_finished().await;
        let mut old_keys: HashSet<K> = self.inner.read().await.keys().cloned().collect();

        for item in iter {
            let key = get_key(&item);
            if !old_keys.remove(key) {
                self.add(key.to_owned(), create_task(item)).await;
            }
        }

        for key in old_keys {
            let result = self.remove(&key).await;
            if let Some(result) = result {
                finished.insert(key, result);
            }
        }

        finished
    }

    pub async fn remove_finished(&self) -> HashMap<K, TaskResult<R>> {
        let mut results = HashMap::new();
        let mut inner = self.inner.write().await;
        for (key, inner_task) in inner.drain_filter(|_, inner_task| inner_task.is_finished()) {
            results.insert(key, inner_task.wait().await);
        }
        results
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
