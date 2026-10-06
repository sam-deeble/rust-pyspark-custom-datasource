//! Decoding for ARINC 429 data words.
//!
//! The crate depends only on `std`. It turns bytes into [`Arinc429Word`]s and
//! knows nothing about Arrow, Python or Spark.
//!
//! ```
//! use arinc429_sdk::{decode_all, params, synth};
//!
//! let bytes = synth::to_bytes(&synth::generate_words(3, 42));
//! for word in decode_all(&bytes)? {
//!     if let Some(param) = params::lookup(word.label_octal()) {
//!         println!("{} = {} {}", param.name, param.decode(&word), param.unit);
//!     }
//! }
//! # Ok::<(), std::io::Error>(())
//! ```

#![warn(missing_docs)]

pub mod params;
pub mod synth;
pub mod word;

mod batch;

pub use batch::decode_all;
pub use word::{Arinc429Word, Ssm};
