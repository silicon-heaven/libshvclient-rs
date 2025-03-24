
#[must_use = "Task has to be used. If you want to detach the task, call .detach() on it."]
pub struct TaskHandle<F: futures::Future + Send + 'static>(
    #[cfg(feature = "tokio")]
    pub tokio::task::JoinHandle<F::Output>,
    #[cfg(feature = "async_std")]
    pub async_std::task::JoinHandle<F::Output>,
    #[cfg(feature = "smol")]
    pub smol::Task<F::Output>,
);

impl<F: futures::Future + Send + 'static> TaskHandle<F> {
    pub async fn cancel(self) {
        #[cfg(feature = "tokio")]
        self.0.abort();
        #[cfg(feature = "async_std")]
        self.0.cancel().await;
        #[cfg(feature = "smol")]
        self.0.cancel().await;
    }

    pub fn detach(self) {
        #[cfg(feature = "smol")]
        self.0.detach();
    }
}

pub fn spawn_task<F>(f: F) -> TaskHandle<F>
where
    F: futures::Future + Send + 'static,
    F::Output: Send + 'static,
{
    #[cfg(feature = "tokio")]
    { TaskHandle(tokio::spawn(f)) }
    #[cfg(feature = "async_std")]
    { TaskHandle(async_std::task::spawn(f)) }
    #[cfg(feature = "smol")]
    { TaskHandle(smol::spawn(f)) }
}
