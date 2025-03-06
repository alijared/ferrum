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
