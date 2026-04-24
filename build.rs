use flate2::read::GzDecoder;
use serde_json::Value;
use std::collections::{BTreeMap, BTreeSet};
use std::env;
use std::fs::File;
use std::io::{Read, Write};
use std::path::PathBuf;

const N: usize = 100_000;
const SCALE: f64 = 8192.0;

const CONT_DIMS: [usize; 6] = [0, 2, 5, 6, 7, 13];
const LOWCARD_DIMS: [usize; 5] = [1, 3, 4, 8, 12];
const LOWCARD_BITS: [u32; 5] = [4, 5, 3, 5, 4];
const LOWCARD_OFFS: [u32; 5] = [0, 4, 9, 12, 17];
const BIN_DIMS: [usize; 3] = [9, 10, 11];
const BIN_OFFS: [u32; 3] = [21, 22, 23];

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
    let mut json_str = String::with_capacity(12 * 1024 * 1024);
    decoder.read_to_string(&mut json_str).expect("gunzip");

    let arr: Vec<Value> = serde_json::from_str(&json_str).expect("parse json");
    assert_eq!(arr.len(), N, "expected {} references, got {}", N, arr.len());

    let mut quantized = vec![0i16; N * 14];
    let mut labels = vec![0u8; N];

    for (i, entry) in arr.iter().enumerate() {
        let vec = entry["vector"].as_array().expect("vector field");
        assert_eq!(vec.len(), 14, "ref {} has {} dims", i, vec.len());

        for (j, v) in vec.iter().enumerate() {
            let f = v.as_f64().expect("numeric vector element");
            quantized[i * 14 + j] = quant(f, j);
        }

        let label = entry["label"].as_str().expect("label field");
        labels[i] = if label == "fraud" { 1 } else { 0 };
    }

    let mut dicts: Vec<Vec<i16>> = Vec::with_capacity(LOWCARD_DIMS.len());
    let mut idx_maps: Vec<BTreeMap<i16, u8>> = Vec::with_capacity(LOWCARD_DIMS.len());

    for (k, &dim) in LOWCARD_DIMS.iter().enumerate() {
        let mut set = BTreeSet::new();
        for i in 0..N {
            set.insert(quantized[i * 14 + dim]);
        }
        let vals: Vec<i16> = set.into_iter().collect();
        let max_card = 1usize << LOWCARD_BITS[k];
        assert!(
            vals.len() <= max_card,
            "dim{:02} cardinality {} exceeds bit width {} (max {})",
            dim, vals.len(), LOWCARD_BITS[k], max_card
        );
        let mut map = BTreeMap::new();
        for (idx, &v) in vals.iter().enumerate() {
            map.insert(v, idx as u8);
        }
        dicts.push(vals);
        idx_maps.push(map);
    }

    let s_i16 = SCALE as i16;
    for &dim in BIN_DIMS.iter() {
        for i in 0..N {
            let v = quantized[i * 14 + dim];
            assert!(
                v == 0 || v == s_i16,
                "dim{:02} non-binary value {} at ref {}", dim, v, i
            );
        }
    }

    let mut packed = vec![0u8; N * 16];
    for i in 0..N {
        let base = i * 16;
        for (k, &dim) in CONT_DIMS.iter().enumerate() {
            let v = quantized[i * 14 + dim].to_le_bytes();
            packed[base + k * 2] = v[0];
            packed[base + k * 2 + 1] = v[1];
        }
        let mut u24: u32 = 0;
        for (k, &dim) in LOWCARD_DIMS.iter().enumerate() {
            let v = quantized[i * 14 + dim];
            let idx = *idx_maps[k].get(&v).expect("index lookup") as u32;
            u24 |= idx << LOWCARD_OFFS[k];
        }
        for (k, &dim) in BIN_DIMS.iter().enumerate() {
            let bit: u32 = if quantized[i * 14 + dim] != 0 { 1 } else { 0 };
            u24 |= bit << BIN_OFFS[k];
        }
        packed[base + 12] = (u24 & 0xFF) as u8;
        packed[base + 13] = ((u24 >> 8) & 0xFF) as u8;
        packed[base + 14] = ((u24 >> 16) & 0xFF) as u8;
        packed[base + 15] = 0;
    }

    for i in 0..N {
        let base = i * 16;
        for (k, &dim) in CONT_DIMS.iter().enumerate() {
            let lo = packed[base + k * 2];
            let hi = packed[base + k * 2 + 1];
            let got = i16::from_le_bytes([lo, hi]);
            let expected = quantized[i * 14 + dim];
            assert_eq!(got, expected, "cont decode mismatch ref {} dim {}", i, dim);
        }
        let u24 = (packed[base + 12] as u32)
            | ((packed[base + 13] as u32) << 8)
            | ((packed[base + 14] as u32) << 16);
        for (k, &dim) in LOWCARD_DIMS.iter().enumerate() {
            let mask = (1u32 << LOWCARD_BITS[k]) - 1;
            let idx = ((u24 >> LOWCARD_OFFS[k]) & mask) as usize;
            let got = dicts[k][idx];
            let expected = quantized[i * 14 + dim];
            assert_eq!(got, expected, "dict decode mismatch ref {} dim {}", i, dim);
        }
        for (k, &dim) in BIN_DIMS.iter().enumerate() {
            let bit = (u24 >> BIN_OFFS[k]) & 1;
            let got = if bit == 1 { s_i16 } else { 0 };
            let expected = quantized[i * 14 + dim];
            assert_eq!(got, expected, "bit decode mismatch ref {} dim {}", i, dim);
        }
    }

    File::create(out_dir.join("packed.bin"))
        .unwrap()
        .write_all(&packed)
        .unwrap();

    File::create(out_dir.join("labels.bin"))
        .unwrap()
        .write_all(&labels)
        .unwrap();

    let mut dict_rs = String::new();
    for (k, &dim) in LOWCARD_DIMS.iter().enumerate() {
        let width = LOWCARD_BITS[k] as usize;
        let slots = 1usize << width;
        let vals = &dicts[k];
        dict_rs.push_str(&format!(
            "pub const DICT{:02}: [i16; {}] = [",
            dim, slots
        ));
        for j in 0..slots {
            let v = if j < vals.len() { vals[j] } else { 0 };
            if j > 0 {
                dict_rs.push_str(", ");
            }
            dict_rs.push_str(&v.to_string());
        }
        dict_rs.push_str("];\n");
    }
    File::create(out_dir.join("dict.rs"))
        .unwrap()
        .write_all(dict_rs.as_bytes())
        .unwrap();

    eprintln!(
        "build.rs: packed {} refs ({} bytes), labels {}, dict cardinalities {:?}",
        N,
        packed.len(),
        labels.len(),
        dicts.iter().map(|d| d.len()).collect::<Vec<_>>()
    );
}
