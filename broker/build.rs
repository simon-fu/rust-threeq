fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("cargo:rerun-if-changed=proto/helloworld.proto");
    tonic_build::compile_protos("proto/helloworld.proto")?;
    
    println!("cargo:rerun-if-changed=proto/cluster.proto");
    tonic_build::compile_protos("proto/cluster.proto")?;

    // println!("cargo:rerun-if-changed=proto/service.proto");
    println!("cargo:rerun-if-changed=proto/zrpc.proto");
    println!("cargo:rerun-if-changed=proto/znodes.proto");
    println!("cargo:rerun-if-changed=proto/threeq.proto");

    // tonic_build::compile_protos("proto/service.proto")?;
    prost_build::compile_protos(&["proto/zrpc.proto", "proto/znodes.proto"], &["proto/"])?;
    prost_build::compile_protos(&["proto/threeq.proto"], &["proto/"])?;

    built::write_built_file().expect("Failed to acquire build-time information");

    Ok(())
}
