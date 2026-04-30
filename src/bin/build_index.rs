use flate2::Compression;
use flate2::read::GzDecoder;
use flate2::write::GzEncoder;
use serde::Deserialize;
use serde::de::{Deserializer as _, SeqAccess, Visitor};
use std::fmt;
use std::fs::File;
use std::io::{BufWriter, Write};
use std::time::Instant;

const K: usize = 4096;
const D: usize = 14;
const N_ITER: usize = 25;

fn main() {
    let t0 = Instant::now();

    eprintln!("loading dataset...");
    let (vectors, labels) = load_dataset();
    let n = vectors.len();
    eprintln!("  {} vectors in {:?}", n, t0.elapsed());

    eprintln!("kmeans++ init (sample={})...", n.min(50_000));
    let t1 = Instant::now();
    let mut centroids = kmeans_plus_plus_init(&vectors, K, 0xdeadbeef_cafebabe_u64);
    eprintln!("  done in {:?}", t1.elapsed());

    eprintln!("lloyd iterations...");
    let mut assignments = vec![0u16; n];
    for iter in 0..N_ITER {
        let t = Instant::now();
        let changed = assign_parallel(&vectors, &centroids, &mut assignments);
        update_centroids(&vectors, &assignments, &mut centroids);
        eprintln!(
            "  iter {:2}: {:5.2}% changed in {:?}",
            iter + 1,
            changed as f64 / n as f64 * 100.0,
            t.elapsed()
        );
        if changed * 1000 < n {
            break;
        }
    }

    eprintln!("writing index...");
    let t2 = Instant::now();
    write_index(&vectors, &labels, &assignments, &centroids, n);
    eprintln!("  written in {:?}", t2.elapsed());
    eprintln!("total: {:?}", t0.elapsed());
}

fn load_dataset() -> (Vec<[f32; D]>, Vec<u8>) {
    let file = File::open("resources/references.json.gz").expect("run from project root");
    let gz = GzDecoder::new(std::io::BufReader::new(file));
    let mut de = serde_json::Deserializer::from_reader(gz);

    #[derive(Deserialize)]
    struct E {
        vector: [f32; D],
        label: String,
    }

    struct V {
        vecs: Vec<[f32; D]>,
        lbls: Vec<u8>,
    }

    impl<'de> Visitor<'de> for V {
        type Value = (Vec<[f32; D]>, Vec<u8>);
        fn expecting(&self, f: &mut fmt::Formatter) -> fmt::Result {
            write!(f, "array")
        }
        fn visit_seq<A: SeqAccess<'de>>(mut self, mut seq: A) -> Result<Self::Value, A::Error> {
            while let Some(e) = seq.next_element::<E>()? {
                self.vecs.push(e.vector);
                self.lbls.push(if e.label == "fraud" { 1 } else { 0 });
            }
            Ok((self.vecs, self.lbls))
        }
    }

    de.deserialize_seq(V {
        vecs: Vec::with_capacity(3_100_000),
        lbls: Vec::with_capacity(3_100_000),
    })
    .expect("json parse error")
}

struct Lcg(u64);

impl Lcg {
    fn new(seed: u64) -> Self {
        Lcg(seed)
    }
    fn next_u64(&mut self) -> u64 {
        self.0 = self
            .0
            .wrapping_mul(6364136223846793005)
            .wrapping_add(1442695040888963407);
        self.0
    }
    fn next_usize(&mut self, n: usize) -> usize {
        (self.next_u64() >> 33) as usize % n
    }
    fn next_f64(&mut self) -> f64 {
        (self.next_u64() >> 11) as f64 / (1u64 << 53) as f64
    }
}

fn dist_sq(a: &[f32; D], b: &[f32; D]) -> f32 {
    let mut d = 0.0f32;
    for i in 0..D {
        let diff = a[i] - b[i];
        d += diff * diff;
    }
    d
}

fn kmeans_plus_plus_init(vectors: &[[f32; D]], k: usize, seed: u64) -> Vec<[f32; D]> {
    let n = vectors.len();
    let mut rng = Lcg::new(seed);
    let sample_size = n.min(50_000);
    let sample: Vec<usize> = (0..sample_size).map(|_| rng.next_usize(n)).collect();

    let mut centroids: Vec<[f32; D]> = Vec::with_capacity(k);
    centroids.push(vectors[sample[rng.next_usize(sample_size)]]);

    let mut min_dists = vec![f32::INFINITY; sample_size];

    for _ in 1..k {
        let last = *centroids.last().unwrap();
        for (i, &vi) in sample.iter().enumerate() {
            let d = dist_sq(&vectors[vi], &last);
            if d < min_dists[i] {
                min_dists[i] = d;
            }
        }
        let total: f64 = min_dists.iter().map(|&x| x as f64).sum();
        let r = rng.next_f64() * total;
        let mut cum = 0.0f64;
        let mut chosen = sample_size - 1;
        for (i, &d) in min_dists.iter().enumerate() {
            cum += d as f64;
            if cum >= r {
                chosen = i;
                break;
            }
        }
        centroids.push(vectors[sample[chosen]]);
    }
    centroids
}

fn nearest_centroid(v: &[f32; D], centroids: &[[f32; D]]) -> u16 {
    let mut best_dist = f32::INFINITY;
    let mut best_idx = 0u16;
    for (i, c) in centroids.iter().enumerate() {
        let d = dist_sq(v, c);
        if d < best_dist {
            best_dist = d;
            best_idx = i as u16;
        }
    }
    best_idx
}

fn assign_parallel(vectors: &[[f32; D]], centroids: &[[f32; D]], assignments: &mut [u16]) -> usize {
    let n_threads = std::thread::available_parallelism()
        .map(|x| x.get())
        .unwrap_or(4)
        .min(16);
    let chunk = (vectors.len() + n_threads - 1) / n_threads;
    let total_changed = std::sync::atomic::AtomicUsize::new(0);

    std::thread::scope(|s| {
        for (v_chunk, a_chunk) in vectors.chunks(chunk).zip(assignments.chunks_mut(chunk)) {
            let tc = &total_changed;
            s.spawn(move || {
                let mut changed = 0usize;
                for (v, a) in v_chunk.iter().zip(a_chunk.iter_mut()) {
                    let best = nearest_centroid(v, centroids);
                    if best != *a {
                        changed += 1;
                        *a = best;
                    }
                }
                tc.fetch_add(changed, std::sync::atomic::Ordering::Relaxed);
            });
        }
    });

    total_changed.load(std::sync::atomic::Ordering::Relaxed)
}

fn update_centroids(vectors: &[[f32; D]], assignments: &[u16], centroids: &mut [[f32; D]]) {
    let k = centroids.len();
    let mut sums = vec![[0.0f64; D]; k];
    let mut counts = vec![0u32; k];
    for (v, &a) in vectors.iter().zip(assignments.iter()) {
        let ci = a as usize;
        counts[ci] += 1;
        for d in 0..D {
            sums[ci][d] += v[d] as f64;
        }
    }
    for i in 0..k {
        if counts[i] == 0 {
            continue;
        }
        for d in 0..D {
            centroids[i][d] = (sums[i][d] / counts[i] as f64) as f32;
        }
    }
}

fn write_index(
    vectors: &[[f32; D]],
    labels: &[u8],
    assignments: &[u16],
    centroids: &[[f32; D]],
    n: usize,
) {
    let k = centroids.len();

    let mut cluster_vecs: Vec<Vec<usize>> = vec![vec![]; k];
    for (i, &a) in assignments.iter().enumerate() {
        cluster_vecs[a as usize].push(i);
    }

    let mut block_offsets = vec![0u32; k + 1];
    for ci in 0..k {
        let sz = cluster_vecs[ci].len() as u32;
        block_offsets[ci + 1] = block_offsets[ci] + (sz + 7) / 8;
    }
    let total_blocks = block_offsets[k] as usize;
    let padded_n = total_blocks * 8;

    let mut out_labels = vec![0u8; padded_n];
    let mut out_blocks = vec![0i16; total_blocks * 112];

    for ci in 0..k {
        let block_start = block_offsets[ci] as usize;
        let vecs = &cluster_vecs[ci];
        let n_blocks = (block_offsets[ci + 1] - block_offsets[ci]) as usize;

        for bk in 0..n_blocks {
            let block_base = (block_start + bk) * 112;
            let label_base = (block_start + bk) * 8;
            for slot in 0..8 {
                match vecs.get(bk * 8 + slot) {
                    Some(&vi) => {
                        for d in 0..D {
                            out_blocks[block_base + d * 8 + slot] = quantize(vectors[vi][d]);
                        }
                        out_labels[label_base + slot] = labels[vi];
                    }
                    None => {
                        for d in 0..D {
                            out_blocks[block_base + d * 8 + slot] = i16::MAX;
                        }
                    }
                }
            }
        }
    }

    let mut centroids_t = vec![0.0f32; D * k];
    for ci in 0..k {
        for d in 0..D {
            centroids_t[d * k + ci] = centroids[ci][d];
        }
    }

    std::fs::create_dir_all("data").expect("create data dir");
    let path = "data/index.bin.gz";
    let file = File::create(path).expect("create index.bin.gz");
    let mut w = GzEncoder::new(BufWriter::new(file), Compression::best());

    w.write_all(b"IVF1").unwrap();
    write_u32(&mut w, n as u32);
    write_u32(&mut w, k as u32);
    write_u32(&mut w, D as u32);
    write_f32s(&mut w, &centroids_t);
    for &o in &block_offsets {
        write_u32(&mut w, o);
    }
    w.write_all(&out_labels).unwrap();
    write_i16s(&mut w, &out_blocks);
    w.flush().unwrap();

    let meta = std::fs::metadata(path).unwrap();
    eprintln!(
        "  index.bin.gz: {:.1}MB (padded_n={}, total_blocks={})",
        meta.len() as f64 / 1_000_000.0,
        padded_n,
        total_blocks
    );
}

fn write_u32<W: Write>(w: &mut W, v: u32) {
    w.write_all(&v.to_le_bytes()).unwrap();
}

fn write_f32s<W: Write>(w: &mut W, data: &[f32]) {
    let bytes = unsafe { std::slice::from_raw_parts(data.as_ptr() as *const u8, data.len() * 4) };
    w.write_all(bytes).unwrap();
}

fn write_i16s<W: Write>(w: &mut W, data: &[i16]) {
    let bytes = unsafe { std::slice::from_raw_parts(data.as_ptr() as *const u8, data.len() * 2) };
    w.write_all(bytes).unwrap();
}

fn quantize(v: f32) -> i16 {
    (v * 10_000.0).round() as i16
}
