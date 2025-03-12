use crate::io::tables;
use crate::server::opentelemetry::collector::logs::v1::logs_service_server::{
    LogsService, LogsServiceServer,
};
use crate::server::opentelemetry::collector::logs::v1::{
    ExportLogsServiceRequest, ExportLogsServiceResponse,
};
use crate::server::opentelemetry::logs::v1::LogRecord;
use crate::server::ServerError;
use log::{error, info};
use tokio::sync::broadcast;
use tokio_util::sync::CancellationToken;
use tonic::transport::Server;
use tonic::{async_trait, Request, Response, Status};

struct LogService {
    bus: broadcast::Sender<Vec<(u64, LogRecord)>>,
}

impl LogService {
    pub fn new(bus: broadcast::Sender<Vec<(u64, LogRecord)>>) -> Self {
        Self { bus }
    }
}

#[async_trait]
impl LogsService for LogService {
    async fn export(
        &self,
        request: Request<ExportLogsServiceRequest>,
    ) -> Result<Response<ExportLogsServiceResponse>, Status> {
        let mut logs = Vec::new();
        for rl in &request.get_ref().resource_logs {
            for sl in &rl.scope_logs {
                for log in &sl.log_records {
                    logs.push((tables::logs::generate_log_id(), log.clone()));
                }
            }
        }

        if let Err(e) = self.bus.send(logs) {
            error!("Error sending record batch over channel: {}", e);
            return Err(Status::internal("Unexpected internal error"));
        }

        Ok(Response::new(ExportLogsServiceResponse {
            partial_success: None,
        }))
    }
}

pub async fn run_server(
    port: u32,
    write_bus: broadcast::Sender<Vec<(u64, LogRecord)>>,
    cancellation_token: CancellationToken,
) -> Result<(), ServerError> {
    let addr = format!("0.0.0.0:{}", port)
        .parse()
        .map_err(ServerError::ParseAddr)?;
    let service = LogService::new(write_bus);

    info!("gRPC server listening on {}", addr);
    Server::builder()
        .add_service(LogsServiceServer::new(service))
        .serve_with_shutdown(addr, async move {
            cancellation_token.cancelled().await;
            info!("Shutting down gRPC server")
        })
        .await
        .map_err(ServerError::Grpc)
}
