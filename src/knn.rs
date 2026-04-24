use crate::data;
use std::arch::x86_64::*;

pub struct Top5 {
    pub idx: [u32; 5],
}

pub fn warmup() {
    let warm = [0i16; 16];
    let _ = knn5(&warm);
}

pub fn knn5(query: &[i16; 16]) -> Top5 {
    unsafe { knn5_avx2(query) }
}

#[target_feature(enable = "avx2")]
unsafe fn knn5_avx2(query: &[i16; 16]) -> Top5 {
    let q = _mm256_loadu_si256(query.as_ptr() as *const __m256i);

    let refs_ptr = data::refs().as_ptr() as *const __m256i;

    let mut td = [u32::MAX; 5];
    let mut ti = [0u32; 5];
    let mut threshold = u32::MAX;

    let n = data::N;
    let mut i = 0usize;
    while i < n {
        if i & 1 == 0 && i + 4 < n {
            _mm_prefetch(refs_ptr.add(i + 4) as *const i8, _MM_HINT_T0);
        }

        let r = _mm256_load_si256(refs_ptr.add(i));
        let diff = _mm256_sub_epi16(q, r);
        let sq = _mm256_madd_epi16(diff, diff);

        let lo = _mm256_castsi256_si128(sq);
        let hi = _mm256_extracti128_si256(sq, 1);
        let s = _mm_add_epi32(lo, hi);
        let s = _mm_add_epi32(s, _mm_srli_si128(s, 8));
        let s = _mm_add_epi32(s, _mm_srli_si128(s, 4));
        let dist = _mm_cvtsi128_si32(s) as u32;

        if dist < threshold {
            insert(&mut td, &mut ti, dist, i as u32);
            threshold = td[4];
        }

        i += 1;
    }

    Top5 { idx: ti }
}

#[inline(always)]
fn insert(td: &mut [u32; 5], ti: &mut [u32; 5], d: u32, i: u32) {
    let mut pos = 5usize;
    let mut k = 0;
    while k < 5 {
        if d < td[k] {
            pos = k;
            break;
        }
        k += 1;
    }
    if pos < 5 {
        let mut k = 4;
        while k > pos {
            td[k] = td[k - 1];
            ti[k] = ti[k - 1];
            k -= 1;
        }
        td[pos] = d;
        ti[pos] = i;
    }
}

#[inline(always)]
pub fn fraud_count(idx: &[u32; 5]) -> u8 {
    let l = data::labels();
    l[idx[0] as usize]
        + l[idx[1] as usize]
        + l[idx[2] as usize]
        + l[idx[3] as usize]
        + l[idx[4] as usize]
}
