fn main() -> Result<(), Box<dyn std::error::Error>> {
    // tonic_build::compile_protos("proto/api.proto")?;
    let out_dir = "src";
    tonic_build::configure()
        .out_dir(out_dir)
        .compile_protos(&["proto/api.proto"], &["proto"])?;
    Ok(())
} 