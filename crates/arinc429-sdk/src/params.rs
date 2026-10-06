//! Example parameter definitions.
//!
//! Real systems take labels and scaling from an interface control document.
//! These two are made up for the demo.

use crate::word::Arinc429Word;

/// A BNR-encoded parameter carried on a given label.
#[derive(Debug, Clone, Copy)]
pub struct ParameterDef {
    /// The label the parameter is carried on, e.g. `0o203`.
    pub label_octal: u8,
    /// Parameter name, used as the `parameter` column value.
    pub name: &'static str,
    /// Engineering unit of the decoded value.
    pub unit: &'static str,
    /// Engineering units per least significant bit.
    pub resolution: f64,
}

/// Altitude in feet on label 203.
pub const ALTITUDE: ParameterDef = ParameterDef {
    label_octal: 0o203,
    name: "altitude",
    unit: "ft",
    resolution: 1.0,
};

/// Airspeed in knots on label 206.
pub const AIRSPEED: ParameterDef = ParameterDef {
    label_octal: 0o206,
    name: "airspeed",
    unit: "kt",
    resolution: 0.125,
};

/// Every parameter [`lookup`] recognises.
pub const KNOWN_PARAMETERS: &[ParameterDef] = &[ALTITUDE, AIRSPEED];

impl ParameterDef {
    /// Decodes `word`'s data field into engineering units.
    pub fn decode(&self, word: &Arinc429Word) -> f64 {
        word.decode_bnr(self.resolution)
    }
}

/// Returns the parameter carried on `label_octal`, if it is a known one.
pub fn lookup(label_octal: u8) -> Option<&'static ParameterDef> {
    KNOWN_PARAMETERS
        .iter()
        .find(|p| p.label_octal == label_octal)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn lookup_finds_known_labels() {
        assert_eq!(lookup(0o203).unwrap().name, "altitude");
        assert_eq!(lookup(0o206).unwrap().name, "airspeed");
        assert!(lookup(0o377).is_none());
    }

    #[test]
    fn decodes_with_resolution() {
        let word = Arinc429Word::from_raw(8 << 10);
        assert_eq!(AIRSPEED.decode(&word), 1.0);
    }
}
