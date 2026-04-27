use flate2::read::GzDecoder;
use serde_json::Value;
use std::env;
use std::fs::File;
use std::io::{Read, Write};
use std::path::PathBuf;

const N: usize = 1_000_000;
const STRIDE: usize = 16;
const SCALE: f64 = 8192.0;

fn quant(v: f64, dim: usize) -> i16 {
    if (dim == 5 || dim == 6) && v < 0.0 {
        return -(SCALE as i16);
    }
    if dim == 9 || dim == 10 || dim == 11 {
        return if v > 0.5 { SCALE as i16 } else { 0 };
    }
    let c = v.max(0.0).min(1.0);
    (c * SCALE + 0.5) as i16
}

fn main() {
    let manifest = PathBuf::from(env::var("CARGO_MANIFEST_DIR").unwrap());
    let out_dir = PathBuf::from(env::var("OUT_DIR").unwrap());

    let gz_path = manifest.join("resources/references.json.gz");
    println!("cargo:rerun-if-changed={}", gz_path.display());
    println!("cargo:rerun-if-changed=build.rs");

    let file = File::open(&gz_path).unwrap_or_else(|e| panic!("open {}: {}", gz_path.display(), e));
    let mut decoder = GzDecoder::new(file);
    let mut json_str = String::with_capacity(128 * 1024 * 1024);
    decoder.read_to_string(&mut json_str).expect("gunzip");

    let arr: Vec<Value> = serde_json::from_str(&json_str).expect("parse json");
    assert_eq!(arr.len(), N, "expected {} references, got {}", N, arr.len());

    let mut pairs: Vec<([i16; STRIDE], u8)> = Vec::with_capacity(N);

    for (i, entry) in arr.iter().enumerate() {
        let vec = entry["vector"].as_array().expect("vector field");

        assert_eq!(vec.len(), 14, "ref {} has {} dims", i, vec.len());

        let mut v = [0i16; STRIDE];
        for (j, val) in vec.iter().enumerate() {
            let f = val.as_f64().expect("numeric vector element");
            v[j] = quant(f, j);
        }

        let label = entry["label"].as_str().expect("label field");
        let l = if label == "fraud" { 1u8 } else { 0u8 };
        pairs.push((v, l));
    }

    pairs.sort_unstable_by_key(|(v, _)| -> u32 {
        v[..14].iter().map(|&x| (x as i32 * x as i32) as u32).sum()
    });

    let mut refs = vec![0u8; N * STRIDE * 2];
    let mut labels = vec![0u8; N];

    for (i, (v, l)) in pairs.iter().enumerate() {
        for (j, &dim) in v.iter().enumerate() {
            let q = dim.to_le_bytes();
            let off = (i * STRIDE + j) * 2;
            refs[off] = q[0];
            refs[off + 1] = q[1];
        }
        labels[i] = *l;
    }

    File::create(out_dir.join("refs.bin"))
        .unwrap()
        .write_all(&refs)
        .unwrap();

    File::create(out_dir.join("labels.bin"))
        .unwrap()
        .write_all(&labels)
        .unwrap();

    eprintln!(
        "build.rs: wrote {} refs ({} bytes) + {} labels",
        N,
        refs.len(),
        labels.len()
    );
}
