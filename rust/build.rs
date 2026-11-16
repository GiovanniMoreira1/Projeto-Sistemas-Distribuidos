fn main() {
    // .proto compartilhado por todas as linguagens, na raiz do repositorio
    println!("cargo:rerun-if-changed=../proto/client.proto");
    prost_build::compile_protos(&["../proto/client.proto"], &["../proto"]).unwrap();
}
