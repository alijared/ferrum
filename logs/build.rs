use std::path::PathBuf;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let proto_files = vec![
        "opentelemetry-proto/opentelemetry/proto/collector/logs/v1/logs_service.proto",
        "opentelemetry-proto/opentelemetry/proto/common/v1/common.proto",
        "opentelemetry-proto/opentelemetry/proto/resource/v1/resource.proto",
        "opentelemetry-proto/opentelemetry/proto/logs/v1/logs.proto",
    ];

    let proto_path: PathBuf = "opentelemetry-proto".into();
    tonic_build::configure()
        .build_server(true)
        .compile_protos(&proto_files, &[proto_path])?;

    let proto_files = vec!["raft-proto/raft.proto"];

    let proto_path: PathBuf = "raft-proto".into();
    tonic_build::configure()
        .build_server(true)
        .compile_protos(&proto_files, &[proto_path])?;

    Ok(())
}
