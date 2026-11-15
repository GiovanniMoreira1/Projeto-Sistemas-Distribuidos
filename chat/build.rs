fn main() {
    prost_build::compile_protos(&["client.proto"], &["."]).unwrap();
}
