pub const N: usize = 1_000_000;
pub const STRIDE: usize = 16;

#[repr(C, align(64))]
struct AlignedRefs([u8; N * STRIDE * 2]);

static REFS_BYTES: AlignedRefs =
    AlignedRefs(*include_bytes!(concat!(env!("OUT_DIR"), "/refs.bin")));
static LABELS_BYTES: &[u8; N] = include_bytes!(concat!(env!("OUT_DIR"), "/labels.bin"));

#[inline(always)]
pub fn refs() -> *const i16 {
    REFS_BYTES.0.as_ptr() as *const i16
}

#[inline(always)]
pub fn labels() -> &'static [u8; N] {
    LABELS_BYTES
}
