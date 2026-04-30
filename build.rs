fn main() {
    println!("cargo:rerun-if-changed=data/index.bin.gz");
    println!("cargo:rerun-if-changed=build.rs");
}
