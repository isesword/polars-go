use std::path::PathBuf;

fn main() {
    // 创建输出目录
    let out_dir = PathBuf::from("src").join("proto");
    std::fs::create_dir_all(&out_dir).expect("Failed to create proto output directory");

    // 获取 proto 文件路径
    let proto_file = PathBuf::from("..")
        .join("proto")
        .join("polars_bridge.proto");
    let proto_dir = PathBuf::from("..").join("proto");

    println!("cargo:rerun-if-changed={}", proto_file.display());

    prost_build::Config::new()
        .out_dir(&out_dir)
        .compile_protos(&[proto_file], &[proto_dir])
        .expect("Failed to compile protobuf");
}
