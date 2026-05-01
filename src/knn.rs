use crate::data::{self, Dataset};
use std::arch::x86_64::*;
use std::mem::MaybeUninit;

const FAST_NPROBE: usize = 16;
const FULL_NPROBE: usize = 24;
const MAX_CENTROIDS: usize = 4096;
const VECTOR_SCALE: f32 = 0.0001;

pub fn knn5_fraud_count(query: &[f32; 14], ds: &Dataset) -> u8 {
    unsafe { knn5_ivf(query, ds) }
}

pub fn warmup() {
    let ds = data::dataset();
    let mut state = 0x12345678u32;
    for _ in 0..50 {
        let mut q = [0.0f32; 14];
        for v in q.iter_mut() {
            state = state.wrapping_mul(1664525).wrapping_add(1013904223);
            *v = (state >> 8) as f32 / (1u32 << 24) as f32;
        }
        let _ = knn5_fraud_count(&q, ds);
    }
}

#[target_feature(enable = "avx2,fma")]
unsafe fn knn5_ivf(query: &[f32; 14], ds: &Dataset) -> u8 {
    let mut dists = [MaybeUninit::<f32>::uninit(); MAX_CENTROIDS];
    compute_centroid_dists(query, ds, &mut dists);

    let mut q_vecs = [_mm256_setzero_ps(); 14];
    for d in 0..14usize {
        q_vecs[d] = _mm256_set1_ps(query[d]);
    }

    let fast_probes = top_n_from_dists::<FAST_NPROBE>(&dists, ds.k);
    let fast = scan_and_count(&fast_probes, ds, &q_vecs);

    if fast != 2 && fast != 3 {
        return fast;
    }

    let full_probes = top_n_from_dists::<FULL_NPROBE>(&dists, ds.k);
    scan_and_count(&full_probes, ds, &q_vecs)
}

#[target_feature(enable = "avx2,fma")]
unsafe fn compute_centroid_dists(
    query: &[f32; 14],
    ds: &Dataset,
    dists: &mut [MaybeUninit<f32>; MAX_CENTROIDS],
) {
    let k = ds.k;
    let cp = ds.centroids.as_ptr();
    let dp = dists.as_mut_ptr() as *mut f32;

    {
        let qd = _mm256_set1_ps(query[0]);
        let mut ci = 0usize;
        while ci + 16 <= k {
            let d0 = _mm256_sub_ps(_mm256_loadu_ps(cp.add(ci)), qd);
            let d1 = _mm256_sub_ps(_mm256_loadu_ps(cp.add(ci + 8)), qd);
            _mm256_storeu_ps(dp.add(ci), _mm256_mul_ps(d0, d0));
            _mm256_storeu_ps(dp.add(ci + 8), _mm256_mul_ps(d1, d1));
            ci += 16;
        }
        while ci + 8 <= k {
            let d0 = _mm256_sub_ps(_mm256_loadu_ps(cp.add(ci)), qd);
            _mm256_storeu_ps(dp.add(ci), _mm256_mul_ps(d0, d0));
            ci += 8;
        }
        while ci < k {
            let diff = *cp.add(ci) - query[0];
            *dp.add(ci) = diff * diff;
            ci += 1;
        }
    }

    for d in 1..14usize {
        let base = d * k;
        let qd = _mm256_set1_ps(query[d]);
        let mut ci = 0usize;
        while ci + 16 <= k {
            let cv0 = _mm256_loadu_ps(cp.add(base + ci));
            let cv1 = _mm256_loadu_ps(cp.add(base + ci + 8));
            let d0 = _mm256_sub_ps(cv0, qd);
            let d1 = _mm256_sub_ps(cv1, qd);
            let a0 = _mm256_loadu_ps(dp.add(ci));
            let a1 = _mm256_loadu_ps(dp.add(ci + 8));
            _mm256_storeu_ps(dp.add(ci), _mm256_fmadd_ps(d0, d0, a0));
            _mm256_storeu_ps(dp.add(ci + 8), _mm256_fmadd_ps(d1, d1, a1));
            ci += 16;
        }
        while ci + 8 <= k {
            let cv = _mm256_loadu_ps(cp.add(base + ci));
            let d0 = _mm256_sub_ps(cv, qd);
            let a0 = _mm256_loadu_ps(dp.add(ci));
            _mm256_storeu_ps(dp.add(ci), _mm256_fmadd_ps(d0, d0, a0));
            ci += 8;
        }
        while ci < k {
            let diff = *cp.add(base + ci) - query[d];
            *dp.add(ci) += diff * diff;
            ci += 1;
        }
    }
}

#[target_feature(enable = "avx2,fma")]
unsafe fn top_n_from_dists<const N: usize>(
    dists: &[MaybeUninit<f32>; MAX_CENTROIDS],
    k: usize,
) -> [usize; N] {
    let mut top_dists = [f32::INFINITY; N];
    let mut top_idx = [0usize; N];
    let dp = dists.as_ptr() as *const f32;
    let mut ci = 0usize;

    while ci + 8 <= k {
        let d8 = _mm256_loadu_ps(dp.add(ci));
        let mask = _mm256_movemask_ps(_mm256_cmp_ps(
            d8,
            _mm256_set1_ps(top_dists[N - 1]),
            _CMP_LT_OQ,
        )) as u32;

        if mask != 0 {
            let mut buf = [0.0f32; 8];
            _mm256_storeu_ps(buf.as_mut_ptr(), d8);
            let mut m = mask;
            while m != 0 {
                let s = m.trailing_zeros() as usize;
                m &= m - 1;
                let di = buf[s];
                if di < top_dists[N - 1] {
                    let pos = top_dists.partition_point(|&x| x < di);
                    top_dists[pos..N].rotate_right(1);
                    top_dists[pos] = di;
                    top_idx[pos..N].rotate_right(1);
                    top_idx[pos] = ci + s;
                }
            }
        }
        ci += 8;
    }

    while ci < k {
        let di = *dp.add(ci);
        if di < top_dists[N - 1] {
            let pos = top_dists.partition_point(|&x| x < di);
            top_dists[pos..N].rotate_right(1);
            top_dists[pos] = di;
            top_idx[pos..N].rotate_right(1);
            top_idx[pos] = ci;
        }
        ci += 1;
    }

    top_idx
}

#[target_feature(enable = "avx2,fma")]
unsafe fn scan_and_count(probes: &[usize], ds: &Dataset, q_vecs: &[__m256; 14]) -> u8 {
    let mut top: [(f32, u8); 5] = [(f32::INFINITY, 0); 5];
    let mut worst_idx = 0usize;
    let blocks_ptr = ds.blocks.as_ptr();
    let labels_ptr = ds.labels.as_ptr();

    for &ci in probes {
        let start = *ds.offsets.as_ptr().add(ci) as usize;
        let end = *ds.offsets.as_ptr().add(ci + 1) as usize;
        scan_blocks(
            q_vecs,
            blocks_ptr,
            labels_ptr,
            start,
            end,
            &mut top,
            &mut worst_idx,
        );
    }

    top.iter().filter(|(_, l)| *l == 1).count() as u8
}

#[target_feature(enable = "avx2,fma")]
unsafe fn scan_blocks(
    q_vecs: &[__m256; 14],
    blocks_ptr: *const i16,
    labels_ptr: *const u8,
    start_block: usize,
    end_block: usize,
    top: &mut [(f32, u8); 5],
    worst_idx: &mut usize,
) {
    let scale = _mm256_set1_ps(VECTOR_SCALE);

    macro_rules! dim_pair {
        ($acc0:expr, $acc1:expr, $bb:expr, $d:expr) => {{
            let r0 = _mm_loadu_si128(blocks_ptr.add($bb + $d * 8) as *const __m128i);
            let r1 = _mm_loadu_si128(blocks_ptr.add($bb + ($d + 1) * 8) as *const __m128i);
            let v0 = _mm256_mul_ps(_mm256_cvtepi32_ps(_mm256_cvtepi16_epi32(r0)), scale);
            let v1 = _mm256_mul_ps(_mm256_cvtepi32_ps(_mm256_cvtepi16_epi32(r1)), scale);
            let d0 = _mm256_sub_ps(v0, q_vecs[$d]);
            let d1 = _mm256_sub_ps(v1, q_vecs[$d + 1]);
            $acc0 = _mm256_fmadd_ps(d0, d0, $acc0);
            $acc1 = _mm256_fmadd_ps(d1, d1, $acc1);
        }};
    }

    'block: for block_i in start_block..end_block {
        let prefetch_block = block_i + 8;
        if prefetch_block < end_block {
            _mm_prefetch(
                blocks_ptr.add(prefetch_block * 112) as *const i8,
                _MM_HINT_T0,
            );
            _mm_prefetch(
                blocks_ptr.add(prefetch_block * 112 + 56) as *const i8,
                _MM_HINT_T0,
            );
        }

        let bb = block_i * 112;
        let threshold = _mm256_set1_ps(top[*worst_idx].0);

        let mut acc0 = _mm256_setzero_ps();
        let mut acc1 = _mm256_setzero_ps();

        dim_pair!(acc0, acc1, bb, 0);
        dim_pair!(acc0, acc1, bb, 2);
        dim_pair!(acc0, acc1, bb, 4);
        dim_pair!(acc0, acc1, bb, 6);

        let partial = _mm256_add_ps(acc0, acc1);
        if _mm256_movemask_ps(_mm256_cmp_ps(partial, threshold, _CMP_LT_OQ)) == 0 {
            continue 'block;
        }

        dim_pair!(acc0, acc1, bb, 8);
        dim_pair!(acc0, acc1, bb, 10);
        dim_pair!(acc0, acc1, bb, 12);

        let acc = _mm256_add_ps(acc0, acc1);
        let mut mask = _mm256_movemask_ps(_mm256_cmp_ps(acc, threshold, _CMP_LT_OQ)) as u32;
        if mask == 0 {
            continue;
        }

        let mut dists_buf = [0.0f32; 8];
        _mm256_storeu_ps(dists_buf.as_mut_ptr(), acc);
        let label_base = block_i * 8;
        while mask != 0 {
            let slot = mask.trailing_zeros() as usize;
            mask &= mask - 1;
            let di = dists_buf[slot];
            if di < top[*worst_idx].0 {
                top[*worst_idx] = (di, *labels_ptr.add(label_base + slot));
                let mut wi = 0;
                let mut wv = top[0].0;
                for j in 1..5 {
                    if top[j].0 > wv {
                        wv = top[j].0;
                        wi = j;
                    }
                }
                *worst_idx = wi;
            }
        }
    }
}
