//! Strict Ironwood note-commitment extraction from compact blocks.

pub mod parser;

pub use parser::{extract_commitments, ironwood_prior_tree_size, ParseError};
