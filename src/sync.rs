#[allow(unused)]
#[cfg(all(not(feature = "shuttle"), test))]
pub(crate) use std::thread::JoinHandle;

#[cfg(not(feature = "shuttle"))]
pub(crate) use std::thread::spawn;

#[cfg(feature = "shuttle")]
pub(crate) use shuttle::thread::spawn;

#[cfg(feature = "shuttle")]
#[inline]
pub(crate) fn cooperative_yield() {
    shuttle::thread::yield_now();
}

#[cfg(not(feature = "shuttle"))]
#[inline]
pub(crate) fn cooperative_yield() {}

#[cfg(feature = "shuttle")]
pub(crate) use shuttle::sync::*;

#[cfg(feature = "shuttle")]
#[allow(unused_imports)]
pub(crate) use shuttle::thread;

#[cfg(not(feature = "shuttle"))]
pub(crate) use std::sync::*;

#[cfg(not(feature = "shuttle"))]
#[allow(unused_imports)]
pub(crate) use std::thread;
