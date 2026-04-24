use std::sync::OnceLock;

pub const N: usize = 100_000;
pub const STRIDE: usize = 16;

#[repr(C, align(64))]
pub struct AlignedRefs(pub [i16; N * STRIDE]);

static REFS: OnceLock<Box<AlignedRefs>> = OnceLock::new();
static LABELS_BYTES: &[u8; N] = include_bytes!(concat!(env!("OUT_DIR"), "/labels.bin"));
static REFS_BYTES: &[u8; N * STRIDE * 2] = include_bytes!(concat!(env!("OUT_DIR"), "/refs.bin"));

pub fn init() {
    let mut boxed: Box<AlignedRefs> = Box::new(AlignedRefs([0i16; N * STRIDE]));

    unsafe {
        std::ptr::copy_nonoverlapping(
            REFS_BYTES.as_ptr(),
            boxed.0.as_mut_ptr() as *mut u8,
            N * STRIDE * 2,
        );
    }

    REFS.set(boxed)
        .map_err(|_| "REFS already initialized")
        .unwrap();
}

#[inline(always)]
pub fn refs() -> &'static [i16; N * STRIDE] {
    &REFS.get().expect("call data::init() first").0
}

#[inline(always)]
pub fn labels() -> &'static [u8; N] {
    LABELS_BYTES
}
