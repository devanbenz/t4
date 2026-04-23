mod common;
pub(crate) mod error;
pub(crate) mod io_task;
pub(crate) mod io_worker;
pub(crate) mod sync;

#[cfg(feature = "io-uring")]
#[cfg(target_os = "linux")]
pub(crate) mod io_uring;

// TODO: Uncomment this after working on it
// #[cfg(feature = "kqueue")]
#[cfg(target_os = "macos")]
pub(crate) mod kqueue;
