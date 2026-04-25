use std::fs::File;

use crossbeam_channel::{Receiver, Select, Sender, bounded, unbounded};

use crate::io::common::{BackendLoop, CompletionEvent, IoDriver, SubmissionEntry};
use crate::io::error::{Error, Result};
use crate::io::io_task::WorkerRequest;
use crate::io::sync::{mpsc, thread};

struct IoJob {
    fd: i32,
    user_data: u64,
    kind: IoJobKind,
}

enum IoJobKind {
    Read {
        buf: *mut u8,
        len: usize,
        offset: u64,
    },
    Write {
        buf: *const u8,
        len: usize,
        offset: u64,
    },
    Fsync,
}

unsafe impl Send for IoJob {}

impl IoJob {
    fn execute(&self) -> i32 {
        unsafe {
            let ret: isize = match self.kind {
                IoJobKind::Read { buf, len, offset } => {
                    libc::pread(self.fd, buf.cast(), len, offset as libc::off_t)
                }
                IoJobKind::Write { buf, len, offset } => {
                    libc::pwrite(self.fd, buf.cast(), len, offset as libc::off_t)
                }
                IoJobKind::Fsync => libc::fsync(self.fd) as isize,
            };
            if ret < 0 {
                -std::io::Error::last_os_error().raw_os_error().unwrap_or(0)
            } else {
                ret as i32
            }
        }
    }
}

pub(crate) struct GenericIoDriver {
    queue_size: usize,
    inflight: usize,
    job_tx: Sender<IoJob>,
    completion_rx: Receiver<CompletionEvent>,
}

impl GenericIoDriver {
    fn new(queue_size: usize) -> Result<Self> {
        let num_threads = std::thread::available_parallelism()
            .map(|n| n.get())?
            .min(queue_size.max(1));
        let (job_tx, job_rx) = bounded::<IoJob>(queue_size);
        let (completion_tx, completion_rx) = unbounded::<CompletionEvent>();

        for _ in 0..num_threads {
            let job_rx = job_rx.clone();
            let completion_tx = completion_tx.clone();
            thread::spawn(move || worker_loop(job_rx, completion_tx));
        }

        Ok(Self {
            queue_size,
            inflight: 0,
            job_tx,
            completion_rx,
        })
    }
}

impl IoDriver for GenericIoDriver {
    fn available_submission_slots(&mut self) -> usize {
        self.queue_size.saturating_sub(self.inflight)
    }

    fn push(&mut self, entry: SubmissionEntry) -> Result<()> {
        let job = match entry {
            SubmissionEntry::Read {
                fd,
                buf_ptr,
                buf_len,
                offset,
                user_data,
            } => IoJob {
                fd,
                user_data,
                kind: IoJobKind::Read {
                    buf: buf_ptr,
                    len: buf_len as usize,
                    offset,
                },
            },
            SubmissionEntry::Write {
                fd,
                buf_ptr,
                buf_len,
                offset,
                user_data,
                is_last_in_batch: _,
            } => IoJob {
                fd,
                user_data,
                kind: IoJobKind::Write {
                    buf: buf_ptr,
                    len: buf_len as usize,
                    offset,
                },
            },
            SubmissionEntry::Fsync { fd, user_data } => IoJob {
                fd,
                user_data,
                kind: IoJobKind::Fsync,
            },
        };
        self.job_tx.try_send(job).map_err(|_| {
            Error::Io(std::io::Error::new(
                std::io::ErrorKind::WouldBlock,
                "submission queue is full",
            ))
        })?;
        self.inflight += 1;
        Ok(())
    }

    fn submit(&mut self) -> Result<()> {
        Ok(())
    }

    fn pop_completion(&mut self) -> Option<CompletionEvent> {
        match self.completion_rx.try_recv() {
            Ok(event) => {
                self.inflight = self.inflight.saturating_sub(1);
                Some(event)
            }
            Err(_) => None,
        }
    }

    fn wait_for_progress(&mut self, request_rx: &mpsc::Receiver<WorkerRequest>) {
        let mut sel = Select::new();
        sel.recv(request_rx);
        sel.recv(&self.completion_rx);
        sel.ready();
    }
}

fn worker_loop(job_rx: Receiver<IoJob>, completion_tx: Sender<CompletionEvent>) {
    while let Ok(job) = job_rx.recv() {
        let event = CompletionEvent {
            user_data: job.user_data,
            result: job.execute(),
        };
        if completion_tx.send(event).is_err() {
            return;
        }
    }
}

pub(crate) fn new_backend(
    file: File,
    queue_depth: u32,
    rx: mpsc::Receiver<WorkerRequest>,
) -> Result<BackendLoop<GenericIoDriver>> {
    let queue_depth = queue_depth as usize;
    let driver = GenericIoDriver::new(queue_depth)?;
    Ok(BackendLoop::new(file, queue_depth, rx, driver))
}
