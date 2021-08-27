fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("cargo:rerun-if-changed=proto/service.proto");
    println!("cargo:rerun-if-changed=proto/zrpc.proto");
    println!("cargo:rerun-if-changed=proto/znodes.proto");

    tonic_build::compile_protos("proto/service.proto")?;
    prost_build::compile_protos(&["proto/zrpc.proto", "proto/znodes.proto"], &["proto/"])?;
    Ok(())
}
