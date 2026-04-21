use std::fs::File;
use std::num::NonZero;

use crate::Result;

#[derive(Debug)]
pub(crate) struct IoWorker {}

impl IoWorker {
    pub(crate) fn new(queue_depth: NonZero<u32>, file: File) -> Result<Self> {
        todo!()
    }
    pub(crate) fn fsync() {
        todo!()
    }

    pub(crate) fn clone(&self) -> Self {
        todo!()
    }
}
