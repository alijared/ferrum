# Ferrum logs: A high-performance log management system

## Overview

Ferrum is a sophisticated log management system built in Rust, designed to handle log ingestion from opentelemetry, 
storage, querying, and 
processing using modern data processing techniques. The project consists of two main Rust crates: ferum-ql 
(a query language parser) and logs (the main log management application).
 
## Key components

### Log ingestion and storage
The logs system is built around several components:

1. Data model. Logs are stored with a rich schema including:

    * Timestamp (nanosecond precision)
    * Log level
    * Message
    * Attributes (as a map)
    * Day partitioning

2. Table management (`logs/src/io/tables`):
    * Uses Apache Arrow and DataFusion for 
high-performance data processing
    * Implements compaction and periodic file management
    * Enables efficient querying and storage of log data using parquet

### Query Language (ferum-ql or fql). 
Located in ferum-ql/src/lib.rs, this is a custom query language parser using the 
nom parsing library. It allows users to:

* Filter logs by level, message, and custom attributes
* Apply operations like equality, inequality, and regex matching
* Perform functions like counting and JSON transformation (message field)

Example query syntax:
``
{level="Error", message~="connection.*", myattribute="something"} | count
``

This query would:

1. Filter logs with Error level 
2. Match messages with a "connection" regex pattern
3. Count the results

#### Key Design Features:

* Strongly typed parsing
* Supports multiple filter operations
* Flexible attribute filtering
* Function extensions (count, JSON conversion for message fields)

### Servers
The application provides two server interfaces:

#### gRPC server (`logs/src/server/grpc.rs`)
1. Implements the OpenTelemetry `LogsService`
2. Receives log entries from distributed systems
3. Transforms incoming logs into the internal data model
4. High-performance log ingestion

#### HTTP API server (`logs/src/server/http/`)
1. `/query/fql`: Ferum Query Language endpoint
2. `/query/sql`: Direct SQL querying
3. `/query/attributes`: Query labels based on FQL query

### Configuration management
The system supports flexible configuration via YAML:
* Data directory configuration
* Log table settings (e.g., compaction frequency)
* Server port configurations

## Performance Considerations

* Uses jemallocator for memory management
* Leverages Apache Arrow for columnar data processing
* Implements zero-copy data transformations
* Supports parallel processing with configurable partitions

## Deployment and Runtime

* Supports graceful shutdown
* Configurable log levels
* Environment-aware configuration loading
* Handles system signals (SIGTERM, CTRL+C)

## Example Workflow

1. A distributed system sends logs via gRPC
2. Logs are parsed and stored efficiently
3. Users can query logs using Ferum Query Language or standard SQL
4. Results can be returned as counts or detailed log entries based on filters
