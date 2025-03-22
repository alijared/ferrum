#[global_allocator]
static GLOBAL: jemallocator::Jemalloc = jemallocator::Jemalloc;

use crate::io::tables;
use crate::raft::Raft;
use crate::server::grpc::raft::raft_proto;
use crate::server::{grpc, http};
use clap::Parser;
use futures_util::try_join;
use log::{error, info};
use std::process::exit;
use std::time::Duration;
use tokio::signal;
use tokio::sync::broadcast;
use tokio_util::sync::CancellationToken;

mod config;
mod io;
mod raft;
mod server;
mod udfs;

#[derive(Parser, Debug)]
#[command(version, about = "Ferrum logs", long_about = None)]
struct CliArgs {
    #[arg(
        short,
        long,
        help = "Config file location",
        default_value = config::DEFAULT_CONFIG_PATH
    )]
    config_file: String,

    #[arg(long, help = "Application log level", default_value_t = log::LevelFilter::Info)]
    log_level: log::LevelFilter,
}

#[tokio::main]
async fn main() {
    let cli_args = CliArgs::parse();
    env_logger::builder()
        .filter_level(cli_args.log_level)
        .init();

    let config = match config::load(&cli_args.config_file) {
        Ok(c) => c,
        Err(e) => {
            error!("Failed to load configuration: {}", e);
            exit(1);
        }
    };

    let raft_server_port = config.replication.advertise_port;
    let replica_config = config.replication;
    let replica_timeout = replica_config.connect_timeout;
    let replicas = replica_config.replicas.clone();
    let (logs_write_tx, logs_write_rx) = broadcast::channel(4096);
    let raft = match Raft::new(replica_config, logs_write_tx).await {
        Ok(r) => r,
        Err(e) => {
            error!("Error creating Raft: {}", e);
            exit(1);
        }
    };

    let cancellation_token = CancellationToken::new();
    shutdown(cancellation_token.clone());

    let session_ctx = io::set_session_context().await;
    let logs_handle = match tables::logs::initialize(
        &session_ctx,
        config.data_dir.clone(),
        Duration::from_secs(config.log_table_config.compaction_frequency_seconds),
        logs_write_rx.resubscribe(),
        cancellation_token.clone(),
    )
    .await
    {
        Ok(h) => h,
        Err(e) => {
            error!("Failed to initialize logs table: {}", e);
            exit(1);
        }
    };

    let log_attr_handle = match tables::log_attributes::initialize(
        &session_ctx,
        config.data_dir,
        Duration::from_secs(config.log_table_config.compaction_frequency_seconds),
        logs_write_rx,
        cancellation_token.clone(),
    )
    .await
    {
        Ok(h) => h,
        Err(e) => {
            error!("Failed to initialize log attributes table: {}", e);
            exit(1);
        }
    };

    let server_config = config.server;
    let api_server = http::run_server(server_config.http.port, cancellation_token.clone());
    let otel_grpc_server = grpc::run_server(
        "OpenTelemetry",
        server_config.grpc.port,
        grpc::opentelemetry::collector::logs::v1::logs_service_server::LogsServiceServer::new(
            grpc::opentelemetry::LogService::new(&replicas, raft.clone()),
        ),
        cancellation_token.clone(),
    );
    let raft_grpc_server = grpc::run_server(
        "Raft",
        raft_server_port,
        raft_proto::raft_service_server::RaftServiceServer::new(grpc::raft::Service::new(
            raft.clone(),
        )),
        cancellation_token.clone(),
    );

    let cancel = cancellation_token.clone();
    tokio::spawn(async move {
        if let Err(e) = raft
            .connect_replicas(Duration::from_secs(replica_timeout), &replicas)
            .await
        {
            error!("Failed to connect to replicas: {}", e);
            cancel.cancel();
        }
    });

    match try_join!(api_server, otel_grpc_server, raft_grpc_server) {
        Ok(_) => {}
        Err(e) => {
            cancellation_token.clone().cancel();
            match e {
                server::Error::ParseAddr(e) => error!("Failed to parse server addr: {}", e),
                server::Error::Http(e) => error!("Error running HTTP server: {}", e),
                server::Error::Grpc(e) => error!("Error running gRPC server: {}", e),
                _ => unreachable!(),
            }
            exit(1);
        }
    }

    match try_join!(logs_handle, log_attr_handle) {
        Ok(_) => {}
        Err(e) => {
            error!("Failed to join table futures: {}", e);
            exit(1);
        }
    }
}

fn shutdown(token: CancellationToken) {
    tokio::spawn(async move {
        let mut terminate = signal::unix::signal(signal::unix::SignalKind::terminate()).unwrap();
        tokio::select! {
            _ = signal::ctrl_c() => {},
            _ = terminate.recv() => {},
        }
        info!("Shutting down services");
        token.cancel();
    });
}
