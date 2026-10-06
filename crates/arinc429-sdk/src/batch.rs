use std::io;

use crate::word::Arinc429Word;

/// Decodes a buffer of back-to-back 32-bit little-endian words.
///
/// # Errors
///
/// Returns [`io::ErrorKind::InvalidData`] if the buffer length is not a
/// multiple of 4.
pub fn decode_all(buf: &[u8]) -> io::Result<Vec<Arinc429Word>> {
    if !buf.len().is_multiple_of(4) {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("buffer length {} is not a multiple of 4", buf.len()),
        ));
    }
    let (words, _) = buf.as_chunks::<4>();
    Ok(words
        .iter()
        .map(|bytes| Arinc429Word::from_raw(u32::from_le_bytes(*bytes)))
        .collect())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::synth;

    #[test]
    fn rejects_misaligned_buffers() {
        assert!(decode_all(&[0u8; 5]).is_err());
    }

    #[test]
    fn decodes_empty_buffer() {
        assert_eq!(decode_all(&[]).unwrap(), vec![]);
    }

    #[test]
    fn round_trips_synthetic_words() {
        let words = synth::generate_words(500, 1);
        let decoded = decode_all(&synth::to_bytes(&words)).unwrap();
        assert_eq!(decoded, words);
    }
}
