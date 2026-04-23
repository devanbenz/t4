use std::collections::HashMap;
use std::fs::File;
use std::os::fd::AsRawFd;

use ringbuf::traits::{Observer, Producer};

use crate::buffer::AlignedBuf;
use crate::io::common::{
    CompletionEvent, InflightRequest, InflightRequestKind, RequestId, complete_request_with_error,
    decode_cqe_result, decode_user_data, encode_user_data, request_op_count, worker_failed_error,
};
use crate::io::error::{Error, Result};
use crate::io::io_task::WorkerRequest;
use crate::io::sync::mpsc;

pub(super) struct IoJob {
    pub(super) fd: i32,
    pub(super) user_data: u64,
    pub(super) kind: IoJobKind,
}

pub(super) enum IoJobKind {
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
    pub(super) fn execute(&self) -> i32 {
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
                -(*libc::__error())
            } else {
                ret as i32
            }
        }
    }
}

struct KqueueDriver {
    // TODO: replace with { pool_tx, kq_fd, completion_deque } once the
    // thread-pool backend lands. Kept as a ringbuf for now so the sketch
    // still type-checks against the current submit path.
    buffer: ringbuf::HeapRb<IoJob>,
}

trait IoDriver {
    fn available_submission_slots(&mut self) -> usize;
    fn push(&mut self, job: IoJob) -> Result<()>;
    fn submit(&mut self) -> Result<usize>;
    fn pop_completion(&mut self) -> Option<CompletionEvent>;
}

struct ReadEntry<'a> {
    fd: i32,
    buf: &'a mut AlignedBuf,
    offset: u64,
    user_data: u64,
}

impl From<ReadEntry<'_>> for IoJob {
    fn from(v: ReadEntry<'_>) -> Self {
        Self {
            fd: v.fd,
            user_data: v.user_data,
            kind: IoJobKind::Read {
                buf: v.buf.as_mut_ptr(),
                len: v.buf.len(),
                offset: v.offset,
            },
        }
    }
}

struct WriteEntry<'a> {
    fd: i32,
    buf: &'a AlignedBuf,
    offset: u64,
    user_data: u64,
}

impl From<WriteEntry<'_>> for IoJob {
    fn from(v: WriteEntry<'_>) -> Self {
        Self {
            fd: v.fd,
            user_data: v.user_data,
            kind: IoJobKind::Write {
                buf: v.buf.as_ptr(),
                len: v.buf.len(),
                offset: v.offset,
            },
        }
    }
}

struct FsyncEntry {
    fd: i32,
    user_data: u64,
}

impl From<FsyncEntry> for IoJob {
    fn from(v: FsyncEntry) -> Self {
        Self {
            fd: v.fd,
            user_data: v.user_data,
            kind: IoJobKind::Fsync,
        }
    }
}

impl KqueueDriver {
    fn new(queue_depth: usize) -> Result<Self> {
        Ok(Self {
            buffer: ringbuf::HeapRb::<KqueueEntry>::new(queue_depth),
        })
    }

    fn push_entry(&mut self, entry: KqueueEntry) -> Result<()> {
        self.buffer.try_push(entry).map_err(|_| {
            Error::Io(std::io::Error::new(
                std::io::ErrorKind::WouldBlock,
                "submission queue is full",
            ))
        })?;
        Ok(())
    }
}

impl IoDriver for KqueueDriver {
    fn available_submission_slots(&mut self) -> usize {
        self.buffer.occupied_len() - self.buffer.vacant_len()
    }

    fn push(&mut self, entry: KqueueEntry) -> Result<()> {
        self.push_entry(entry)
    }

    fn submit(&mut self) -> Result<usize> {
        Ok(self.ring.submit()?)
    }

    fn pop_completion(&mut self) -> Option<CompletionEvent> {
        let mut cq = self.ring.completion();
        cq.next().map(|cqe| CompletionEvent {
            user_data: cqe.user_data(),
            result: cqe.result(),
        })
    }
}

pub(crate) struct KqueueBackend<D: IoDriver> {
    receiver: mpsc::Receiver<WorkerRequest>,
    file: File,
    ring: D,
    queue_depth: usize,
    inflight_requests: HashMap<RequestId, InflightRequest>,
    next_request_id: RequestId,
}

impl KqueueBackend<KqueueDriver> {
    pub(crate) fn new(
        file: File,
        queue_depth: usize,
        rx: mpsc::Receiver<WorkerRequest>,
    ) -> Result<Self> {
        let ring = KqueueDriver::new(queue_depth)?;
        Ok(KqueueBackend::with_driver(
            file,
            queue_depth as usize,
            rx,
            ring,
        ))
    }
}

impl<D: IoDriver> KqueueBackend<D> {
    fn with_driver(
        file: File,
        queue_depth: usize,
        rx: mpsc::Receiver<WorkerRequest>,
        ring: D,
    ) -> Self {
        Self {
            receiver: rx,
            file,
            ring,
            queue_depth,
            inflight_requests: HashMap::new(),
            next_request_id: 0,
        }
    }

    pub fn run(mut self) {
        self.thread_loop();
    }
}

impl<D: IoDriver> KqueueBackend<D> {
    fn thread_loop(&mut self) {
        let mut pending_request = None;
        loop {
            let disconnected = match self.submit_requests(&mut pending_request) {
                Ok(disconnected) => disconnected,
                Err(err) => {
                    self.fail_all(err, pending_request.take());
                    return;
                }
            };
            if disconnected {
                return;
            }

            self.poll_completions();

            crate::io::sync::cooperative_yield();
        }
    }

    fn submit_requests(&mut self, pending_request: &mut Option<WorkerRequest>) -> Result<bool> {
        let mut submitted_any = false;

        loop {
            let request = match pending_request.take() {
                Some(request) => request,
                None => match self.receiver.try_recv() {
                    Ok(request) => request,
                    Err(mpsc::TryRecvError::Empty) => break,
                    Err(mpsc::TryRecvError::Disconnected) => return Ok(true),
                },
            };
            let op_count = request_op_count(&request);
            assert!(op_count > 0, "request has no operations");
            if op_count > self.queue_depth {
                complete_request_with_error(
                    request,
                    Error::Io(std::io::Error::new(
                        std::io::ErrorKind::InvalidInput,
                        format!(
                            "request needs {op_count} SQEs, queue depth is {}",
                            self.queue_depth
                        ),
                    )),
                );
                continue;
            }
            if self.ring.available_submission_slots() < op_count {
                *pending_request = Some(request);
                break;
            }
            if self.submit_request(request)? {
                submitted_any = true;
            }
        }

        if submitted_any {
            let _ = self.ring.submit()?;
        }

        Ok(false)
    }

    fn submit_request(&mut self, request: WorkerRequest) -> Result<bool> {
        let request_id = self.allocate_request_id();
        match request {
            WorkerRequest::Read {
                buf,
                offset,
                completion,
            } => {
                self.inflight_requests.insert(
                    request_id,
                    InflightRequest {
                        remaining: 1,
                        error: None,
                        kind: InflightRequestKind::Read {
                            buf: Some(buf),
                            completion,
                            result: None,
                        },
                    },
                );

                let push_result = {
                    let request = self
                        .inflight_requests
                        .get_mut(&request_id)
                        .expect("read request missing after insert");
                    let InflightRequestKind::Read { buf, .. } = &mut request.kind else {
                        unreachable!("request kind changed while submitting read");
                    };
                    self.ring.push(
                        ReadEntry {
                            fd: self.file.as_raw_fd(),
                            buf: buf.as_mut().expect("read buffer missing while submitting"),
                            offset,
                            user_data: encode_user_data(request_id, 0),
                        }
                        .into(),
                    )
                };
                self.finish_single_submit(request_id, push_result)
            }
            WorkerRequest::Fsync { completion } => {
                self.inflight_requests.insert(
                    request_id,
                    InflightRequest {
                        remaining: 1,
                        error: None,
                        kind: InflightRequestKind::Fsync { completion },
                    },
                );

                let push_result = self.ring.push(
                    FsyncEntry {
                        fd: self.file.as_raw_fd(),
                        user_data: encode_user_data(request_id, 0),
                    }
                    .into(),
                );
                self.finish_single_submit(request_id, push_result)
            }
            WorkerRequest::Write { writes, completion } => {
                let page_count = writes.len();
                self.inflight_requests.insert(
                    request_id,
                    InflightRequest {
                        remaining: page_count,
                        error: None,
                        kind: InflightRequestKind::Write {
                            pages: writes,
                            completion,
                        },
                    },
                );

                let mut submitted_pages = 0;
                for index in 0..page_count {
                    let is_last = index + 1 == page_count;
                    let push_result = {
                        let request = self
                            .inflight_requests
                            .get(&request_id)
                            .expect("write request missing after insert");
                        let InflightRequestKind::Write { pages, .. } = &request.kind else {
                            unreachable!("request kind changed while submitting write");
                        };
                        let page = pages
                            .get(index)
                            .expect("write page missing while submitting");
                        self.ring.push(
                            WriteEntry {
                                fd: self.file.as_raw_fd(),
                                buf: &page.buf,
                                offset: page.offset,
                                user_data: encode_user_data(request_id, index),
                                flags: if is_last {
                                    io_uring::squeue::Flags::empty()
                                } else {
                                    io_uring::squeue::Flags::IO_LINK
                                },
                            }
                            .into(),
                        )
                    };

                    match push_result {
                        Ok(()) => {
                            submitted_pages = submitted_pages + 1;
                        }
                        Err(err) => {
                            self.handle_write_submit_error(request_id, submitted_pages, err);
                            return Ok(submitted_pages > 0);
                        }
                    }
                }

                Ok(true)
            }
        }
    }

    fn poll_completions(&mut self) {
        while let Some(cqe) = self.ring.pop_completion() {
            let (request_id, op_index) = decode_user_data(cqe.user_data);
            let mut should_complete = false;

            let Some(request) = self.inflight_requests.get_mut(&request_id) else {
                debug_assert!(false, "missing inflight request for cqe {}", cqe.user_data);
                continue;
            };

            match &mut request.kind {
                InflightRequestKind::Read { result, .. } => {
                    debug_assert_eq!(op_index, 0, "read request should only have op 0");
                    match decode_cqe_result(cqe.result) {
                        Ok(n) => *result = Some(n),
                        Err(err) if request.error.is_none() => request.error = Some(err),
                        Err(_) => {}
                    }
                }
                InflightRequestKind::Write { pages, .. } => {
                    let Some(page) = pages.get(op_index) else {
                        debug_assert!(false, "write page index out of range: {}", op_index);
                        continue;
                    };
                    let expected = page.buf.len();
                    let result = decode_cqe_result(cqe.result).and_then(|n| {
                        if n == expected {
                            Ok(())
                        } else {
                            Err(Error::Io(std::io::Error::new(
                                std::io::ErrorKind::WriteZero,
                                format!("short write: expected {expected}, got {n}"),
                            )))
                        }
                    });
                    if let Err(err) = result
                        && request.error.is_none()
                    {
                        request.error = Some(err);
                    }
                }
                InflightRequestKind::Fsync { .. } => {
                    debug_assert_eq!(op_index, 0, "fsync request should only have op 0");
                    if let Err(err) = decode_cqe_result(cqe.result).map(|_| ())
                        && request.error.is_none()
                    {
                        request.error = Some(err);
                    }
                }
            }

            if request.remaining > 0 {
                request.remaining = request.remaining - 1;
            }
            if request.remaining == 0 {
                should_complete = true;
            }

            if should_complete {
                let request = self
                    .inflight_requests
                    .remove(&request_id)
                    .expect("inflight request missing at completion");
                request.complete();
            }
        }
    }

    fn fail_all(&mut self, err: Error, pending_request: Option<WorkerRequest>) {
        let msg = format!("io worker failed: {err}");

        if let Some(request) = pending_request {
            complete_request_with_error(request, worker_failed_error(msg.clone()));
        }

        for (_, request) in self.inflight_requests.drain() {
            request.complete_with_error(worker_failed_error(msg.clone()));
        }

        while let Ok(request) = self.receiver.try_recv() {
            complete_request_with_error(request, worker_failed_error(msg.clone()));
        }
    }

    fn finish_single_submit(
        &mut self,
        request_id: RequestId,
        push_result: Result<()>,
    ) -> Result<bool> {
        match push_result {
            Ok(()) => Ok(true),
            Err(err) => {
                let request = self
                    .inflight_requests
                    .remove(&request_id)
                    .expect("request missing after failed submit");
                request.complete_with_error(err);
                Ok(false)
            }
        }
    }

    fn handle_write_submit_error(
        &mut self,
        request_id: RequestId,
        submitted_pages: usize,
        err: Error,
    ) {
        if submitted_pages == 0 {
            let request = self
                .inflight_requests
                .remove(&request_id)
                .expect("write request missing after failed first submit");
            request.complete_with_error(err);
            return;
        }

        let request = self
            .inflight_requests
            .get_mut(&request_id)
            .expect("write request missing after partial submit");
        request.remaining = submitted_pages;
        if request.error.is_none() {
            request.error = Some(err);
        }
    }

    fn allocate_request_id(&mut self) -> RequestId {
        loop {
            self.next_request_id = self.next_request_id.wrapping_add(1);
            if !self.inflight_requests.contains_key(&self.next_request_id) {
                return self.next_request_id;
            }
        }
    }
}
