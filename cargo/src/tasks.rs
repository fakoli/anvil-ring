//! Ownership for spawned transport tasks: dropping a parent cancels its child.
pub(crate) struct AbortOnDrop<T> {
    handle: tokio::task::JoinHandle<T>,
}

impl<T> AbortOnDrop<T> {
    pub(crate) fn new(handle: tokio::task::JoinHandle<T>) -> Self {
        Self { handle }
    }
    pub(crate) fn task(&mut self) -> &mut tokio::task::JoinHandle<T> {
        &mut self.handle
    }
}

impl<T> Drop for AbortOnDrop<T> {
    fn drop(&mut self) {
        self.handle.abort();
    }
}
