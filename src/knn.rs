use crate::data::{self, Dataset};
use std::arch::x86_64::*;
use std::mem::MaybeUninit;

const FAST_NPROBE: usize = 16;
const FULL_NPROBE: usize = 24;
const MAX_CENTROIDS: usize = 4096;
const VECTOR_SCALE: f32 = 0.0001;

pub fn knn5_fraud_count(query: &[f32; 14], ds: &Dataset) -> u8 {
    unsafe {
        let fast = knn5_ivf_avx2::<FAST_NPROBE>(query, ds);
        if fast != 2 && fast != 3 {
            return fast;
        }
        knn5_ivf_avx2::<FULL_NPROBE>(query, ds)
    }
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
unsafe fn knn5_ivf_avx2<const NPROBE: usize>(query: &[f32; 14], ds: &Dataset) -> u8 {
    let probes = top_nprobe_centroids_avx2::<NPROBE>(query, ds);

    let mut q_vecs = [_mm256_setzero_ps(); 14];
    for d in 0..14usize {
        q_vecs[d] = _mm256_set1_ps(query[d]);
    }

    let mut top: [(f32, u8); 5] = [(f32::INFINITY, 0); 5];
    let mut worst_idx = 0usize;

    let blocks_ptr = ds.blocks.as_ptr();
    let labels_ptr = ds.labels.as_ptr();

    scan_probes_avx2(
        &probes,
        ds,
        &q_vecs,
        blocks_ptr,
        labels_ptr,
        &mut top,
        &mut worst_idx,
    );

    top.iter().filter(|(_, l)| *l == 1).count() as u8
}

#[target_feature(enable = "avx2,fma")]
unsafe fn top_nprobe_centroids_avx2<const NPROBE: usize>(
    query: &[f32; 14],
    ds: &Dataset,
) -> [usize; NPROBE] {
    let k = ds.k;
    let centroids_ptr = ds.centroids.as_ptr();

    assert!(k <= MAX_CENTROIDS);
    let mut dists = [0.0f32; MAX_CENTROIDS];

    for d in 0..14usize {
        let qd = _mm256_set1_ps(query[d]);
        let base = d * k;
        let mut ci = 0usize;
        while ci + 8 <= k {
            let cv = _mm256_loadu_ps(centroids_ptr.add(base + ci));
            let acc = _mm256_loadu_ps(dists.as_ptr().add(ci));
            let diff = _mm256_sub_ps(cv, qd);
            let new_acc = _mm256_fmadd_ps(diff, diff, acc);
            _mm256_storeu_ps(dists.as_mut_ptr().add(ci), new_acc);
            ci += 8;
        }
        while ci < k {
            let cv = *centroids_ptr.add(base + ci);
            let diff = cv - query[d];
            dists[ci] += diff * diff;
            ci += 1;
        }
    }

    let mut indexed = [MaybeUninit::<(f32, usize)>::uninit(); MAX_CENTROIDS];
    for i in 0..k {
        indexed[i].write((dists[i], i));
    }

    let slice = std::slice::from_raw_parts_mut(indexed.as_mut_ptr() as *mut (f32, usize), k);

    slice.select_nth_unstable_by(NPROBE - 1, |a, b| {
        a.0.partial_cmp(&b.0).unwrap_or(std::cmp::Ordering::Equal)
    });

    slice[..NPROBE]
        .sort_unstable_by(|a, b| a.0.partial_cmp(&b.0).unwrap_or(std::cmp::Ordering::Equal));

    let mut result = [0usize; NPROBE];
    for i in 0..NPROBE {
        result[i] = slice[i].1;
    }
    result
}

#[target_feature(enable = "avx2,fma")]
unsafe fn scan_probes_avx2(
    probes: &[usize],
    ds: &Dataset,
    q_vecs: &[__m256; 14],
    blocks_ptr: *const i16,
    labels_ptr: *const u8,
    top: &mut [(f32, u8); 5],
    worst_idx: &mut usize,
) {
    for &ci in probes {
        let start_block = *ds.offsets.as_ptr().add(ci) as usize;
        let end_block = *ds.offsets.as_ptr().add(ci + 1) as usize;
        scan_blocks_avx2(
            q_vecs,
            blocks_ptr,
            labels_ptr,
            start_block,
            end_block,
            top,
            worst_idx,
        );
    }
}

#[target_feature(enable = "avx2,fma")]
unsafe fn scan_blocks_avx2(
    q_vecs: &[__m256; 14],
    blocks_ptr: *const i16,
    labels_ptr: *const u8,
    start_block: usize,
    end_block: usize,
    top: &mut [(f32, u8); 5],
    worst_idx: &mut usize,
) {
    let scale = _mm256_set1_ps(VECTOR_SCALE);
    for block_i in start_block..end_block {
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
        let block_base = block_i * 112;
        let mut acc0 = _mm256_setzero_ps();
        let mut acc1 = _mm256_setzero_ps();
        for d in (0..14usize).step_by(2) {
            let raw0 = _mm_loadu_si128(blocks_ptr.add(block_base + d * 8) as *const __m128i);
            let raw1 = _mm_loadu_si128(blocks_ptr.add(block_base + (d + 1) * 8) as *const __m128i);
            let v0 = _mm256_mul_ps(_mm256_cvtepi32_ps(_mm256_cvtepi16_epi32(raw0)), scale);
            let v1 = _mm256_mul_ps(_mm256_cvtepi32_ps(_mm256_cvtepi16_epi32(raw1)), scale);
            let diff0 = _mm256_sub_ps(v0, q_vecs[d]);
            let diff1 = _mm256_sub_ps(v1, q_vecs[d + 1]);
            acc0 = _mm256_fmadd_ps(diff0, diff0, acc0);
            acc1 = _mm256_fmadd_ps(diff1, diff1, acc1);
        }
        let acc = _mm256_add_ps(acc0, acc1);
        let closer = _mm256_cmp_ps(acc, _mm256_set1_ps(top[*worst_idx].0), _CMP_LT_OQ);
        let mut mask = _mm256_movemask_ps(closer) as u32;
        if mask == 0 {
            continue;
        }

        let mut dists = [0.0f32; 8];
        _mm256_storeu_ps(dists.as_mut_ptr(), acc);
        let label_base = block_i * 8;
        while mask != 0 {
            let slot = mask.trailing_zeros() as usize;
            mask &= mask - 1;

            let di = dists[slot];
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