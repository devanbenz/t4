#[cfg(feature = "io-uring")]
#[cfg(target_os = "linux")]
pub(crate) mod uring;

// TODO: Enable this when done working on it
// #[cfg(not(feature = "io-uring"))]
pub(crate) mod epoll;

mod common;
mod error;
mod io_task;
