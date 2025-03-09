#[global_allocator]
static GLOBAL: jemallocator::Jemalloc = jemallocator::Jemalloc;

use crate::config::{Config, Error};
use crate::io::tables;
use crate::server::{grpc, http, ServerError};
use clap::Parser;
use futures_util::try_join;
use log::{error, info, warn};
use std::process::exit;
use std::time::Duration;
use tokio::signal;
use tokio::sync::broadcast;
use tokio_util::sync::CancellationToken;

mod config;
mod io;
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

    let cancellation_token = CancellationToken::new();
    shutdown(cancellation_token.clone());

    let session_ctx = io::set_session_context().await;
    let (logs_write_tx, logs_write_rx) = broadcast::channel(4096);
    let config = config::load(cli_args.config_file.as_str()).unwrap_or_else(|e| {
        let msg = match e {
            Error::IO(e) => format!("Failed to read config file: {}", e),
            Error::Parse(e) => format!("Failed to parse config file: {}", e),
        };

        warn!("Failed to load config: {}; using defaults..", msg);
        Config::default()
    });

    let logs_handle = match tables::logs::initialize(
        &session_ctx,
        config.data_dir.to_str().unwrap(),
        Duration::from_secs(config.log_table_config.compaction_frequency_seconds),
        logs_write_rx,
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
        config.data_dir.to_str().unwrap(),
        Duration::from_secs(config.log_table_config.compaction_frequency_seconds),
        logs_write_tx.subscribe(),
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
    let grpc_server = grpc::run_server(
        server_config.grpc.port,
        logs_write_tx,
        cancellation_token.clone(),
    );
    match try_join!(api_server, grpc_server) {
        Ok(_) => {}
        Err(e) => {
            cancellation_token.cancel();
            match e {
                ServerError::ParseAddr(e) => error!("Failed to parse server addr: {}", e),
                ServerError::Http(e) => error!("Error running HTTP server: {}", e),
                ServerError::Grpc(e) => error!("Error running gRPC server: {}", e),
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
