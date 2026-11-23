//! Library half of `overleaf-mount`: the OT engine, the Overleaf API client,
//! the in-memory filesystem image and the sync engine. Kept separate from the
//! binary so the integration tests can drive them without FUSE.

pub mod fsops;
pub mod fuse;
pub mod gitignore;
pub mod merge3;
pub mod ot;
pub mod overleaf;
pub mod ratelimit;
pub mod sync;
pub mod types;
