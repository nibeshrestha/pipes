#[macro_use]
mod error;
mod aggregator;
mod consensus;
mod core;
mod leader;
mod messages;
mod proposer;
mod timer;

#[cfg(test)]
#[path = "tests/common.rs"]
mod common;

pub use crate::consensus::Consensus;
pub use crate::messages::{Block, QC};
