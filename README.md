# Ferrum logs: A high-performance log management system

## Table of Contents
* [Overview](#overview)
* [Key components](#key-components)
  * [Log ingestion and storage](#log-ingestion-and-storage)
  * [Query Language (ferum-ql or fql).](#query-language-ferum-ql-or-fql-)
    * [Key Design Features:](#key-design-features)
  * [Servers](#servers)
    * [OpenTelemetry gRPC server](#opentelemetry-grpc-server)
    * [Internal gRPC server](#internal-grpc-server)
    * [HTTP API server](#http-api-server)
  * [Configuration management](#configuration-management)
* [Performance Considerations](#performance-considerations)
* [Deployment and Runtime](#deployment-and-runtime)
* [Roadmap](#roadmap)
  * [In progress](#in-progress)
  * [Planned (short term)](#planned-short-term)
  * [Planned (long term)](#planned-long-term)

## Overview

Ferrum is a sophisticated log management system built in Rust, designed to handle log ingestion from opentelemetry, 
storage, querying, and 
processing using modern data processing techniques. The project consists of two main Rust crates: ferum-ql 
(a query language parser) and logs (the main log management application).

## General features

- Custom query language (see [FQL](#query-language-ferum-ql-or-fql-) and the [HTTP API](#http-api-server))
- SQL query support (see [HTTP API](#http-api-server))
- Replication with WAL and multi-node support (via [Raft](https://raft.github.io/))
- File compaction (configurable frequency)
- OpenTelemetry logs compatible gRPC server (see [OpenTelemetry gRPC server](#opentelemetry-grpc-server))
- HTTP for querying data (see [HTTP API](#http-api-server))

## Key components

### Log ingestion and storage
The logs system is built around several components:

1. Data model. Logs are stored with a rich schema including
    * Timestamp (nanosecond precision)
    * Log level
    * Message
    * Attributes (as a map)
    * Day partitioning

2. Table management
    * Uses Apache Arrow and DataFusion for 
high-performance data processing
    * Implements compaction and periodic file management
    * Enables efficient querying and storage of log data using parquet

### Query language (ferum-ql or fql). 
Located in ferum-ql crate, this is a custom query language parser build with
[LALRPOP](https://github.com/lalrpop/lalrpop) . It allows users to:

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
The application provides three server interfaces:

#### OpenTelemetry gRPC server

1. Implements the OpenTelemetry `LogsService`
2. Receives log entries from distributed systems
3. Transforms incoming logs into the internal data model
4. High-performance log ingestion

#### Internal gRPC server
Handles replication and internode communication

#### HTTP API server
External API for querying data with the following endpoints:

1. `/query/fql`: Ferum Query Language endpoint
2. `/query/sql`: Direct SQL querying
3. `/query/attributes`: Query attribute keys based on FQL query
4. `/query/attribute/{attribute}/values`: Query attribute values for a specific attribute

### Configuration management
The system supports flexible configuration via YAML:
* Data directory configuration
* Log table settings (e.g., compaction frequency)
* Server port configurations
* Replication settings

See [example config](./logs/config.yaml.example) for details.

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

## Roadmap

### In progress

- [ ] S3 storage backend
- [ ] Improve compaction strategy for better disk usage
- [ ] Docker support

### Planned short-term

- [ ] Distributed queries on multi-node setups
- [ ] Refactor into multiple crates for better benchmarking and testing
- [ ] Add functions to FQL (sum, rate etc.)

### Planned (long-term)

- [ ] Kubernetes operator
- [ ] Data retention policies and TTL's
- [ ] Query caching for improved performance
