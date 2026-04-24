use std::sync::OnceLock;

pub const N: usize = 100_000;

#[repr(C, align(16))]
#[derive(Copy, Clone)]
pub struct PackedRef {
    pub cont: [i16; 6],
    pub packed: [u8; 3],
    pub _pad: u8,
}

const _: () = assert!(std::mem::size_of::<PackedRef>() == 16);
const _: () = assert!(std::mem::align_of::<PackedRef>() == 16);

#[repr(C, align(64))]
pub struct AlignedRefs(pub [PackedRef; N]);

static REFS: OnceLock<Box<AlignedRefs>> = OnceLock::new();
static LABELS_BYTES: &[u8; N] = include_bytes!(concat!(env!("OUT_DIR"), "/labels.bin"));
static PACKED_BYTES: &[u8; N * 16] =
    include_bytes!(concat!(env!("OUT_DIR"), "/packed.bin"));

include!(concat!(env!("OUT_DIR"), "/dict.rs"));

pub fn init() {
    let mut boxed: Box<AlignedRefs> = Box::new(AlignedRefs(
        [PackedRef {
            cont: [0; 6],
            packed: [0; 3],
            _pad: 0,
        }; N],
    ));

    unsafe {
        std::ptr::copy_nonoverlapping(
            PACKED_BYTES.as_ptr(),
            boxed.0.as_mut_ptr() as *mut u8,
            N * 16,
        );
    }

    REFS.set(boxed)
        .map_err(|_| "REFS already initialized")
        .unwrap();
}

#[inline(always)]
pub fn refs() -> &'static [PackedRef; N] {
    &REFS.get().expect("call data::init() first").0
}

#[inline(always)]
pub fn labels() -> &'static [u8; N] {
    LABELS_BYTES
}
