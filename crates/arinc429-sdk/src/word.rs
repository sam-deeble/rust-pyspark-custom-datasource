//! The 32-bit ARINC 429 word.
//!
//! Bit layout, numbered 1 to 32 as in the specification:
//!
//! | Bits  | Field                          |
//! |-------|--------------------------------|
//! | 1-8   | Label (transmitted reversed)   |
//! | 9-10  | Source/Destination Identifier  |
//! | 11-29 | Data                           |
//! | 30-31 | Sign/Status Matrix             |
//! | 32    | Parity (odd)                   |

use std::fmt;

/// A single ARINC 429 word.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct Arinc429Word {
    raw: u32,
}

/// Sign/Status Matrix values, using the BNR interpretation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Ssm {
    /// `0b00`: the source has detected a failure.
    FailureWarning,
    /// `0b01`: no valid data is available.
    NoComputedData,
    /// `0b10`: the data comes from a functional test.
    FunctionalTest,
    /// `0b11`: normal data.
    NormalOperation,
}

impl Arinc429Word {
    /// Wraps a raw 32-bit word.
    pub const fn from_raw(raw: u32) -> Self {
        Self { raw }
    }

    /// The raw 32-bit word.
    pub const fn raw(&self) -> u32 {
        self.raw
    }

    /// Label bits in transmission order. See [`Self::label_octal`] for the usual form.
    pub const fn label_bits(&self) -> u8 {
        (self.raw & 0xFF) as u8
    }

    /// The label as normally written, e.g. `0o203`.
    pub fn label_octal(&self) -> u8 {
        self.label_bits().reverse_bits()
    }

    /// The Source/Destination Identifier (0-3).
    pub const fn sdi(&self) -> u8 {
        ((self.raw >> 8) & 0b11) as u8
    }

    /// The 19-bit data field, right-aligned.
    pub const fn data_field(&self) -> u32 {
        (self.raw >> 10) & 0x7_FFFF
    }

    /// The two Sign/Status Matrix bits.
    pub const fn ssm_bits(&self) -> u8 {
        ((self.raw >> 29) & 0b11) as u8
    }

    /// The Sign/Status Matrix.
    pub fn ssm(&self) -> Ssm {
        match self.ssm_bits() {
            0b00 => Ssm::FailureWarning,
            0b01 => Ssm::NoComputedData,
            0b10 => Ssm::FunctionalTest,
            _ => Ssm::NormalOperation,
        }
    }

    /// The parity bit as transmitted.
    pub const fn parity_bit(&self) -> bool {
        (self.raw >> 31) & 1 == 1
    }

    /// Whether the word has odd parity, as ARINC 429 requires.
    pub fn has_valid_parity(&self) -> bool {
        self.raw.count_ones() % 2 == 1
    }

    /// Interprets the data field as a two's complement BNR value.
    pub fn decode_bnr(&self, resolution: f64) -> f64 {
        const SIGN_BIT: u32 = 1 << 18;
        const FIELD_MASK: u32 = 0x7_FFFF;

        let bits = self.data_field();
        let signed = if bits & SIGN_BIT != 0 {
            (bits | !FIELD_MASK) as i32
        } else {
            bits as i32
        };
        f64::from(signed) * resolution
    }
}

impl fmt::Display for Arinc429Word {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "Arinc429Word {{ label_octal: {:03o}, sdi: {}, data: {}, ssm: {:?}, parity_ok: {} }}",
            self.label_octal(),
            self.sdi(),
            self.data_field(),
            self.ssm(),
            self.has_valid_parity()
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn round_trips_raw_value() {
        let w = Arinc429Word::from_raw(0xDEAD_BEEF);
        assert_eq!(w.raw(), 0xDEAD_BEEF);
    }

    #[test]
    fn label_octal_reflects_bit_order() {
        // 0b1100_0001 on the wire reverses to 0b1000_0011, octal 203.
        let w = Arinc429Word::from_raw(0b1100_0001);
        assert_eq!(w.label_octal(), 0o203);
    }

    #[test]
    fn decode_bnr_handles_negative_values() {
        // All 19 data bits set is -1 in two's complement.
        let raw = 0x7_FFFF << 10;
        let w = Arinc429Word::from_raw(raw);
        assert_eq!(w.decode_bnr(1.0), -1.0);
    }

    #[test]
    fn decode_bnr_handles_positive_values() {
        let raw = 100u32 << 10;
        let w = Arinc429Word::from_raw(raw);
        assert_eq!(w.decode_bnr(0.5), 50.0);
    }

    #[test]
    fn parity_check() {
        let odd = Arinc429Word::from_raw(1 << 31);
        assert!(odd.has_valid_parity());
        let even = Arinc429Word::from_raw(0);
        assert!(!even.has_valid_parity());
    }
}
