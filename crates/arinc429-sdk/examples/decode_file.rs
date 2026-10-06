//! Uses the SDK directly, without Python or Spark.

use arinc429_sdk::{decode_all, params, synth};

fn main() -> std::io::Result<()> {
    let words = synth::generate_words(10_000, 42);
    let path = std::env::temp_dir().join("arinc429_demo.arinc429");
    std::fs::write(&path, synth::to_bytes(&words))?;
    println!(
        "Wrote {} synthetic words to {}",
        words.len(),
        path.display()
    );

    let decoded = decode_all(&std::fs::read(&path)?)?;
    println!("Decoded {} words", decoded.len());

    let mut altitude_count = 0u32;
    let mut airspeed_count = 0u32;
    let mut shown = 0;
    for word in &decoded {
        if let Some(param) = params::lookup(word.label_octal()) {
            match param.name {
                "altitude" => altitude_count += 1,
                "airspeed" => airspeed_count += 1,
                _ => {}
            }
            if shown < 5 {
                println!(
                    "  {} = {:.2} {}",
                    param.name,
                    param.decode(word),
                    param.unit
                );
                shown += 1;
            }
        }
    }
    println!("{altitude_count} altitude words, {airspeed_count} airspeed words");

    Ok(())
}
