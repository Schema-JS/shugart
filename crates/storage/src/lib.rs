use enum_as_inner::EnumAsInner;
use serde::{Deserialize, Serialize};
use thiserror::Error;

mod disk;
mod utils;
mod cursor;
mod disk_metadata;

pub const U64_SIZE: usize = size_of::<u64>();

#[derive(Debug, Clone, EnumAsInner, Serialize, Deserialize, Error, PartialEq)]
pub enum DiskError {
    #[error("The current Disk is already locked")]
    Locked,
    #[error("Could not be flushed")]
    InvalidFlushing,
    #[error("No more bytes allowed")]
    CapacityReached,

}