use crate::io::tables;
use crate::server::opentelemetry::collector::logs::v1::logs_service_server::{
    LogsService, LogsServiceServer,
};
use crate::server::opentelemetry::collector::logs::v1::{
    ExportLogsServiceRequest, ExportLogsServiceResponse,
};
use crate::server::opentelemetry::common::v1::any_value::Value;
use crate::server::opentelemetry::common::v1::AnyValue;
use crate::server::opentelemetry::logs::v1::LogRecord;
use crate::server::ServerError;
use datafusion::arrow::array::{
    Date32Array, MapBuilder, RecordBatch, StringArray, StringBuilder, TimestampNanosecondArray,
};
use datafusion::arrow::datatypes::SchemaRef;
use log::{error, info};
use std::sync::Arc;
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;
use tonic::transport::Server;
use tonic::{async_trait, Request, Response, Status};

struct LogService {
    schema_ref: SchemaRef,
    bus: mpsc::Sender<RecordBatch>,
}

impl LogService {
    pub fn new(bus: mpsc::Sender<RecordBatch>) -> Self {
        Self {
            schema_ref: Arc::new(tables::logs::SCHEMA.clone()),
            bus,
        }
    }
}

#[async_trait]
impl LogsService for LogService {
    async fn export(
        &self,
        request: Request<ExportLogsServiceRequest>,
    ) -> Result<Response<ExportLogsServiceResponse>, Status> {
        let batch = create_record_batch(request.get_ref(), self.schema_ref.clone());
        if let Err(e) = self.bus.send(batch).await {
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
    write_bus: mpsc::Sender<RecordBatch>,
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

fn create_record_batch(request: &ExportLogsServiceRequest, schema_ref: SchemaRef) -> RecordBatch {
    let logs: Vec<LogRecord> = request
        .resource_logs
        .iter()
        .flat_map(|rl| rl.scope_logs.iter().flat_map(|sl| sl.log_records.clone()))
        .collect();

    let cap = logs.len();
    let mut timestamps = Vec::with_capacity(cap);
    let mut levels = Vec::with_capacity(cap);
    let mut messages = Vec::with_capacity(cap);
    let mut days = Vec::with_capacity(cap);
    let mut attributes = MapBuilder::new(None, StringBuilder::new(), StringBuilder::new());

    logs.iter().for_each(|l| {
        let message = l.body.clone().map(convert_any_value).unwrap_or_default();
        let level = l.severity_text.clone();
        let ts = l.time_unix_nano as i64;
        let day = (ts / 86_400_000_000_000) as i32;

        timestamps.push(ts);
        days.push(day);
        levels.push(level);
        messages.push(message);

        l.attributes.iter().for_each(|kv| {
            attributes.keys().append_value(&kv.key);
            let value_str = match &kv.value {
                Some(v) => match &v.value {
                    Some(value) => match value {
                        Value::StringValue(s) => s.clone(),
                        Value::BoolValue(b) => b.to_string(),
                        Value::IntValue(i) => i.to_string(),
                        Value::DoubleValue(d) => d.to_string(),
                        Value::ArrayValue(av) => format!("{:?}", av),
                        Value::KvlistValue(kv) => format!("{:?}", kv),
                        Value::BytesValue(b) => String::from_utf8_lossy(b).to_string(),
                    },
                    None => String::new(),
                },
                None => String::new(),
            };
            attributes.values().append_value(&value_str);
        });
        attributes.append(true).unwrap();
    });

    RecordBatch::try_new(
        schema_ref,
        vec![
            Arc::new(TimestampNanosecondArray::from(timestamps).with_timezone("UTC")),
            Arc::new(StringArray::from(levels)),
            Arc::new(StringArray::from(messages)),
            Arc::new(attributes.finish()),
            Arc::new(Date32Array::from(days)),
        ],
    )
    .unwrap()
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
