fn main() {
    println!("cargo:rerun-if-changed=../proto/client.proto");
    prost_build::compile_protos(&["../proto/client.proto"], &["../proto"]).unwrap();
}
