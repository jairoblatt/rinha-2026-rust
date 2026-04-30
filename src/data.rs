use aligned_vec::{AVec, ConstAlign};
use flate2::read::GzDecoder;
use std::io::Read;
use std::sync::OnceLock;

pub struct Dataset {
    pub centroids: AVec<f32, ConstAlign<32>>,
    pub offsets: Vec<u32>,
    pub labels: Vec<u8>,
    pub blocks: AVec<i16, ConstAlign<32>>,
    pub k: usize,
    pub n: usize,
    pub padded_n: usize,
}

static DATASET: OnceLock<Dataset> = OnceLock::new();

pub fn init() {
    let ds = Dataset::load_embedded().expect("load IVF index");
    if DATASET.set(ds).is_err() {
        panic!("dataset already initialized");
    }
}

pub fn dataset() -> &'static Dataset {
    DATASET.get().expect("dataset not initialized")
}

impl Dataset {
    fn load_embedded() -> Result<Self, Box<dyn std::error::Error>> {
        static INDEX_GZ: &[u8] = include_bytes!("../data/index.bin.gz");
        let mut gz = GzDecoder::new(&INDEX_GZ[..]);

        let mut magic = [0u8; 4];
        gz.read_exact(&mut magic)?;
        if &magic != b"IVF1" {
            return Err("bad magic".into());
        }

        let n = read_u32(&mut gz)? as usize;
        let k = read_u32(&mut gz)? as usize;
        let d = read_u32(&mut gz)? as usize;
        if d != 14 {
            return Err("expected d=14".into());
        }

        let centroids = read_f32_avec(&mut gz, d * k)?;

        let mut offsets = vec![0u32; k + 1];
        for o in offsets.iter_mut() {
            *o = read_u32(&mut gz)?;
        }

        let total_blocks = offsets[k] as usize;
        let padded_n = total_blocks * 8;

        let mut labels = vec![0u8; padded_n];
        gz.read_exact(&mut labels)?;

        let blocks = read_i16_avec(&mut gz, total_blocks * 112)?;

        Ok(Dataset {
            centroids,
            offsets,
            labels,
            blocks,
            k,
            n,
            padded_n,
        })
    }
}

fn read_u32<R: Read>(r: &mut R) -> std::io::Result<u32> {
    let mut b = [0u8; 4];
    r.read_exact(&mut b)?;
    Ok(u32::from_le_bytes(b))
}

fn read_f32_avec<R: Read>(r: &mut R, count: usize) -> std::io::Result<AVec<f32, ConstAlign<32>>> {
    let mut v: AVec<f32, ConstAlign<32>> = AVec::with_capacity(32, count);
    let mut buf = [0u8; 32768];
    let mut remaining = count;
    while remaining > 0 {
        let to_read = (remaining * 4).min(buf.len());
        r.read_exact(&mut buf[..to_read])?;
        for chunk in buf[..to_read].chunks_exact(4) {
            v.push(f32::from_le_bytes([chunk[0], chunk[1], chunk[2], chunk[3]]));
        }
        remaining -= to_read / 4;
    }
    Ok(v)
}

fn read_i16_avec<R: Read>(r: &mut R, count: usize) -> std::io::Result<AVec<i16, ConstAlign<32>>> {
    let mut v: AVec<i16, ConstAlign<32>> = AVec::with_capacity(32, count);
    let mut buf = [0u8; 32768];
    let mut remaining = count;
    while remaining > 0 {
        let to_read = (remaining * 2).min(buf.len());
        r.read_exact(&mut buf[..to_read])?;
        for chunk in buf[..to_read].chunks_exact(2) {
            v.push(i16::from_le_bytes([chunk[0], chunk[1]]));
        }
        remaining -= to_read / 2;
    }
    Ok(v)
}
