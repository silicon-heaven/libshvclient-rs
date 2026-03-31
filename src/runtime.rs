
#[must_use = "Task has to be used. If you want to detach the task, call .detach() on it."]
pub struct TaskHandle<O: Send + 'static>(
// pub struct TaskHandle<F: futures::Future + Send + 'static>(
    #[cfg(feature = "tokio")]
    pub tokio::task::JoinHandle<O>,
    #[cfg(feature = "smol")]
    // The error type is dummy as smol::Task future resolves to the result right away,
    // but we want to keep the same API with tokio JoinHandle, which returns a Result.
    pub smol::Task<Result<O, std::convert::Infallible>>,
);

impl<O: Send + 'static> Future for TaskHandle<O> {
    #[cfg(feature = "tokio")]
    type Output = Result<O, tokio::task::JoinError>;

    #[cfg(feature = "smol")]
    type Output = Result<O, std::convert::Infallible>;

    fn poll(mut self: std::pin::Pin<&mut Self>, cx: &mut std::task::Context<'_>) -> std::task::Poll<Self::Output> {
        std::pin::Pin::new(&mut self.0).poll(cx)
    }
}

// impl<F: futures::Future + Send + 'static> TaskHandle<F> {
impl<O: Send + 'static> TaskHandle<O> {
    #[cfg(feature = "tokio")]
    #[expect(clippy::unused_async, reason = "We want the same API as with smol")]
    pub async fn cancel(self) {
        self.0.abort();
    }

    #[cfg(feature = "smol")]
    pub async fn cancel(self) {
        self.0.cancel().await;
    }

    pub fn detach(self) {
        #[cfg(feature = "smol")]
        self.0.detach();
    }
}

pub fn spawn_task<F>(f: F) -> TaskHandle<F::Output>
where
    F: futures::Future + Send + 'static,
    F::Output: Send + 'static,
{
    #[cfg(feature = "tokio")]
    { TaskHandle(tokio::spawn(f)) }
    #[cfg(feature = "smol")]
    { TaskHandle(smol::spawn(async move { Ok(f.await) } )) }

}

pub fn block_on<T>(future: impl Future<Output = T>) -> T {
    #[cfg(feature = "tokio")]
    { tokio::runtime::Runtime::new().expect("Runtime must work").block_on(future) }

    #[cfg(feature = "smol")]
    { smol::block_on(future) }
}
