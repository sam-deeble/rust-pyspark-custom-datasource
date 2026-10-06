//! Deterministic synthetic data for examples, tests and benchmarks.

use crate::params::{AIRSPEED, ALTITUDE};
use crate::word::Arinc429Word;

/// Generates `count` words that cycle through altitude, airspeed and an
/// unknown label (`0o377`). The same seed always gives the same words.
pub fn generate_words(count: usize, seed: u64) -> Vec<Arinc429Word> {
    let mut rng = Xorshift64::new(seed);
    (0..count)
        .map(|i| {
            let r = rng.next_u64() as u32;
            match i % 3 {
                0 => build_word(ALTITUDE.label_octal, 0, r & 0x7_FFFF, 0b11),
                1 => build_word(AIRSPEED.label_octal, 0, r & 0x7_FFFF, 0b11),
                _ => build_word(
                    0o377,
                    (r & 0b11) as u8,
                    r & 0x7_FFFF,
                    ((r >> 2) & 0b11) as u8,
                ),
            }
        })
        .collect()
}

/// Serializes words in the on-disk format read by [`crate::decode_all`].
pub fn to_bytes(words: &[Arinc429Word]) -> Vec<u8> {
    words.iter().flat_map(|w| w.raw().to_le_bytes()).collect()
}

fn build_word(label_octal: u8, sdi: u8, data: u32, ssm: u8) -> Arinc429Word {
    let mut raw = u32::from(label_octal.reverse_bits());
    raw |= u32::from(sdi & 0b11) << 8;
    raw |= (data & 0x7_FFFF) << 10;
    raw |= u32::from(ssm & 0b11) << 29;
    if raw.count_ones().is_multiple_of(2) {
        raw |= 1 << 31; // set the parity bit so the word has odd parity
    }
    Arinc429Word::from_raw(raw)
}

struct Xorshift64(u64);

impl Xorshift64 {
    fn new(seed: u64) -> Self {
        Self(seed.max(1)) // xorshift never leaves zero
    }

    fn next_u64(&mut self) -> u64 {
        let mut x = self.0;
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        self.0 = x;
        x
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn is_deterministic_for_a_given_seed() {
        assert_eq!(generate_words(50, 123), generate_words(50, 123));
    }

    #[test]
    fn different_seeds_differ() {
        assert_ne!(generate_words(50, 1), generate_words(50, 2));
    }

    #[test]
    fn generated_words_have_valid_parity() {
        for w in generate_words(1000, 42) {
            assert!(w.has_valid_parity(), "{w}");
        }
    }

    #[test]
    fn writes_four_bytes_per_word() {
        assert_eq!(to_bytes(&generate_words(17, 5)).len(), 17 * 4);
    }
}
