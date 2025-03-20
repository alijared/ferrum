use crate::io::tables;
use crate::raft::Raft;

use crate::raft;
use crate::server::grpc::opentelemetry::collector::logs::v1::logs_service_server::LogsService;
use crate::server::grpc::opentelemetry::collector::logs::v1::{
    ExportLogsServiceRequest, ExportLogsServiceResponse,
};
use crate::server::grpc::opentelemetry::common::v1::any_value::Value;
use crate::server::grpc::opentelemetry::common::v1::AnyValue;
use async_raft::async_trait::async_trait;
use log::{error, info};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use tokio::sync::broadcast;
use tonic::{Request, Response, Status};

#[allow(clippy::all)]
pub mod collector {
    #[allow(clippy::all)]
    pub mod logs {
        pub mod v1 {
            tonic::include_proto!("opentelemetry.proto.collector.logs.v1");
        }
    }
}

#[allow(clippy::all)]
pub mod common {
    #[allow(clippy::all)]
    pub mod v1 {
        tonic::include_proto!("opentelemetry.proto.common.v1");
    }
}

#[allow(clippy::all)]
pub mod resource {
    #[allow(clippy::all)]
    pub mod v1 {
        tonic::include_proto!("opentelemetry.proto.resource.v1");
    }
}

#[allow(clippy::all)]
pub mod logs {
    #[allow(clippy::all)]
    pub mod v1 {
        tonic::include_proto!("opentelemetry.proto.logs.v1");
    }
}

#[derive(Clone, Serialize, Deserialize)]
pub struct LogRecord {
    pub level: String,
    pub message: String,
    pub attributes: HashMap<String, String>,
    pub timestamp: i64,
    pub day: i32,
}

impl From<&logs::v1::LogRecord> for LogRecord {
    fn from(log: &logs::v1::LogRecord) -> Self {
        let timestamp = log.time_unix_nano as i64;
        let mut attributes = HashMap::new();
        attributes.insert("level".to_string(), log.severity_text.to_uppercase());
        for kv in &log.attributes {
            let value = kv.value.clone().map(convert_any_value).unwrap_or_default();
            attributes.insert(kv.key.clone(), value);
        }

        Self {
            level: log.severity_text.to_uppercase(),
            message: log.body.clone().map(convert_any_value).unwrap_or_default(),
            attributes,
            timestamp,
            day: (timestamp / 86_400_000_000_000) as i32,
        }
    }
}

pub struct LogService {
    bus: broadcast::Sender<Vec<(u64, LogRecord)>>,
    raft: Raft,
}

impl LogService {
    pub fn new(bus: broadcast::Sender<Vec<(u64, LogRecord)>>, raft: Raft) -> Self {
        Self { bus, raft }
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
                    let (id, record) = (tables::logs::generate_log_id(), LogRecord::from(log));
                    match self
                        .raft
                        .write(raft::Request::new(id, record.clone()))
                        .await
                    {
                        Ok(_) => {
                            info!("would have written logs");
                        },
                        Err(e) => {
                            error!("Unable to send replication request: {}", e);
                        }
                    }
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

fn convert_any_value(value: AnyValue) -> String {
    value.value.map(convert_value).unwrap_or_default()
}

fn convert_value(value: Value) -> String {
    match value {
        Value::StringValue(s) => s,
        Value::BoolValue(b) => b.to_string(),
        Value::IntValue(i) => i.to_string(),
        Value::DoubleValue(d) => d.to_string(),
        Value::ArrayValue(_) => "".to_string(),
        Value::KvlistValue(_) => "".to_string(),
        Value::BytesValue(b) => String::from_utf8(b).unwrap_or_default(),
    }
}
