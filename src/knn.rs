use crate::data::{
    self, PackedRef, DICT01, DICT03, DICT04, DICT08, DICT12,
};
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

#[inline(always)]
fn sq_diff(a: i16, b: i16) -> u32 {
    let d = a as i32 - b as i32;
    (d * d) as u32
}

#[target_feature(enable = "avx2")]
unsafe fn knn5_avx2(query: &[i16; 16]) -> Top5 {
    let mut pd01 = [0u32; 16];
    let mut pd03 = [0u32; 32];
    let mut pd04 = [0u32; 8];
    let mut pd08 = [0u32; 32];
    let mut pd12 = [0u32; 16];

    let q01 = query[1];
    let q03 = query[3];
    let q04 = query[4];
    let q08 = query[8];
    let q12 = query[12];
    let q09 = query[9];
    let q10 = query[10];
    let q11 = query[11];

    for k in 0..16 {
        pd01[k] = sq_diff(q01, DICT01[k]);
    }
    for k in 0..32 {
        pd03[k] = sq_diff(q03, DICT03[k]);
    }
    for k in 0..8 {
        pd04[k] = sq_diff(q04, DICT04[k]);
    }
    for k in 0..32 {
        pd08[k] = sq_diff(q08, DICT08[k]);
    }
    for k in 0..16 {
        pd12[k] = sq_diff(q12, DICT12[k]);
    }

    let pd09: [u32; 2] = [sq_diff(q09, 0), sq_diff(q09, 8192)];
    let pd10: [u32; 2] = [sq_diff(q10, 0), sq_diff(q10, 8192)];
    let pd11: [u32; 2] = [sq_diff(q11, 0), sq_diff(q11, 8192)];

    let q_cont_arr: [i16; 8] = [
        query[0], query[2], query[5], query[6], query[7], query[13], 0, 0,
    ];
    let q_cont = _mm_loadu_si128(q_cont_arr.as_ptr() as *const __m128i);

    let refs_ptr = data::refs().as_ptr() as *const __m128i;

    let mut td = [u32::MAX; 5];
    let mut ti = [0u32; 5];
    let mut threshold = u32::MAX;

    let n = data::N;
    let mut i = 0usize;
    while i < n {
        if i + 8 < n {
            _mm_prefetch(refs_ptr.add(i + 8) as *const i8, _MM_HINT_T0);
        }

        let row = _mm_load_si128(refs_ptr.add(i));

        let ref_cont = _mm_insert_epi32(row, 0, 3);
        let diff = _mm_sub_epi16(q_cont, ref_cont);
        let sq = _mm_madd_epi16(diff, diff);
        let s = _mm_add_epi32(sq, _mm_srli_si128(sq, 8));
        let s = _mm_add_epi32(s, _mm_srli_si128(s, 4));
        let mut dist = _mm_cvtsi128_si32(s) as u32;

        let packed24 = _mm_extract_epi32(row, 3) as u32;
        let i01 = ( packed24       ) & 0x0F;
        let i03 = ( packed24 >>  4 ) & 0x1F;
        let i04 = ( packed24 >>  9 ) & 0x07;
        let i08 = ( packed24 >> 12 ) & 0x1F;
        let i12 = ( packed24 >> 17 ) & 0x0F;
        let b09 = ( packed24 >> 21 ) & 0x01;
        let b10 = ( packed24 >> 22 ) & 0x01;
        let b11 = ( packed24 >> 23 ) & 0x01;

        dist = dist
            .wrapping_add(*pd01.get_unchecked(i01 as usize))
            .wrapping_add(*pd03.get_unchecked(i03 as usize))
            .wrapping_add(*pd04.get_unchecked(i04 as usize))
            .wrapping_add(*pd08.get_unchecked(i08 as usize))
            .wrapping_add(*pd12.get_unchecked(i12 as usize))
            .wrapping_add(*pd09.get_unchecked(b09 as usize))
            .wrapping_add(*pd10.get_unchecked(b10 as usize))
            .wrapping_add(*pd11.get_unchecked(b11 as usize));

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

#[allow(dead_code)]
#[inline]
pub(crate) fn decode_i16(row: &PackedRef, dim: usize) -> i16 {
    let u24 = (row.packed[0] as u32)
        | ((row.packed[1] as u32) << 8)
        | ((row.packed[2] as u32) << 16);
    match dim {
        0 => row.cont[0],
        2 => row.cont[1],
        5 => row.cont[2],
        6 => row.cont[3],
        7 => row.cont[4],
        13 => row.cont[5],
        1 => DICT01[((u24) & 0x0F) as usize],
        3 => DICT03[((u24 >> 4) & 0x1F) as usize],
        4 => DICT04[((u24 >> 9) & 0x07) as usize],
        8 => DICT08[((u24 >> 12) & 0x1F) as usize],
        12 => DICT12[((u24 >> 17) & 0x0F) as usize],
        9 => if ((u24 >> 21) & 1) == 1 { 8192 } else { 0 },
        10 => if ((u24 >> 22) & 1) == 1 { 8192 } else { 0 },
        11 => if ((u24 >> 23) & 1) == 1 { 8192 } else { 0 },
        _ => 0,
    }
}
