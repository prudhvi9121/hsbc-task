# High-Throughput IoT Telemetry Pipeline

A high-performance IoT event streaming pipeline built using **Java 17**, **Apache Kafka (KRaft)**, and **Docker**.

The application generates **50 million synthetic telemetry records**, streams them through Kafka, classifies each record, and routes them to multiple Kafka topics while operating within a strict resource limit of **2 GB RAM** and **4 CPU cores**.

The entire project is fully containerized. **Docker Desktop is the only prerequisite**—no Java, Maven, or Kafka installation is required.

---

# Features

- Generates **50 million** synthetic telemetry records
- Streams records through Apache Kafka
- Routes records to multiple Kafka topics based on business rules
- Archives every processed record
- Fully automated build, execution, benchmarking, and verification
- Runs entirely inside Docker
- Optimized for high throughput and low memory usage

---

# Technology Stack

| Component | Technology |
|-----------|------------|
| Language | Java 17 |
| Messaging | Apache Kafka (KRaft) |
| Build Tool | Maven |
| Containerization | Docker & Docker Compose |
| Serialization | CSV |
| Testing | JUnit |

---

# Project Structure

```text
iot-telemetry-pipeline/
│
├── src/
│   ├── main/
│   └── test/
│
├── scripts/
│   ├── build.sh
│   ├── build.ps1
│   ├── run.sh
│   ├── run.ps1
│   ├── verify.sh
│   ├── verify.ps1
│   ├── benchmark.sh
│   └── benchmark.ps1
│
├── Dockerfile
├── docker-compose.yml
├── pom.xml
└── README.md
```

---

# Prerequisites

Install only:

- Docker Desktop

Everything else (Java, Maven, Kafka, dependencies) is handled automatically inside Docker.

---

# Quick Start

## 1. Build

Builds the Docker image and packages the application.

### Windows

```powershell
.\scripts\build.ps1
```

### Linux / macOS

```bash
./scripts/build.sh
```

---

## 2. Run

Starts Kafka, creates the required topics, generates **50 million telemetry records**, and processes the complete pipeline.

### Windows

```powershell
.\scripts\run.ps1
```

### Linux / macOS

```bash
./scripts/run.sh
```

---

## 3. Verify

Validates that every record has been routed correctly.

### Windows

```powershell
.\scripts\verify.ps1
```

### Linux / macOS

```bash
./scripts/verify.sh
```

Example output:

```text
====================================================================

Check 1:
Critical + Nominal == Source           [PASS]

Check 2:
Regional Archive == Source             [PASS]

FINAL RESULT : PASS

====================================================================
```

---

# System Architecture

```text
                    Telemetry Generator
                           │
                           ▼
                    Kafka Source Topic
                           │
                           ▼
                  Parallel Kafka Consumers
                           │
                           ▼
                  Message Router / Classifier
                    ┌─────────┴─────────┐
                    │                   │
                    ▼                   ▼
              Critical Topic      Nominal Topic
                    │
                    └──────────────┐
                                   ▼
                         Regional Archive
```

---

# Processing Flow

1. Generate telemetry records on demand.
2. Publish records to the **source** Kafka topic.
3. Consume records using parallel Kafka consumers.
4. Classify records based on the first character of the `readingType`.
5. Route records to the appropriate destination topic.
6. Archive every processed record.
7. Verify record counts after processing.

---

# Routing Rules

| Condition | Destination Topic |
|-----------|-------------------|
| Reading type starts with **A–M** | `critical` |
| Reading type starts with **N–Z** | `nominal` |
| Every record | `regional_archive` |

The `regional_archive` topic is partitioned alphabetically by region:

| Partition | Regions |
|-----------|---------|
| Partition 0 | Africa, Asia |
| Partition 1 | Australia, Europe |
| Partition 2 | North America, South America |

---

# Performance Optimizations

The application is designed to maximize throughput while staying within the assignment's resource constraints.

### Efficient Record Generation

- Records are generated on demand.
- No large in-memory buffering.
- Constant memory usage regardless of record count.

### Optimized CSV Parsing

- Avoids `String.split()` and regular expressions.
- Uses direct index scanning.
- Minimizes object creation and garbage collection.

### Parallel Processing

- One Kafka consumer per source partition.
- Parallel routing using dedicated worker threads.
- Manual partition assignment for predictable processing.

### Kafka Producer Optimization

- LZ4 compression
- 64 KB producer batches
- Configured linger time for efficient batching

### JVM Optimization

- G1 Garbage Collector
- Tuned heap configuration
- Low GC pause times
- Zero Full GC during benchmark execution

---

# Performance Results

The application was benchmarked under the required resource limits.

| Metric | Result |
|---------|--------|
| Total Records | 50,000,000 |
| Producer Throughput | ~812,000 records/sec |
| Consumer Throughput | ~638,000 records/sec |
| Peak JVM Heap | ~420 MB |
| Full GC Events | 0 |
| Maximum GC Pause | < 9 ms |

---

# Resource Constraints

The complete solution runs within:

- **Memory:** 2 GB
- **CPU:** 4 Cores

Memory allocation:

| Component | Heap Size |
|-----------|-----------|
| Kafka Broker | 256 MB |
| Java Application | 512 MB – 1024 MB |

---

# Design Decisions

### Streaming Instead of Buffering

Rather than generating all 50 million records first, records are generated and streamed directly to Kafka. This keeps memory usage constant and allows the application to scale efficiently.

### Kafka KRaft Mode

Kafka runs in KRaft mode without ZooKeeper, simplifying deployment and reducing resource usage.

### Parallel Consumers

Each source partition is processed by a dedicated consumer thread, improving throughput while avoiding unnecessary thread contention.

### Stateless Routing

Routing decisions depend only on the current message, allowing efficient parallel processing without shared state.

---

# Verification

The verification script confirms that all records have been processed correctly by checking:

- **Critical + Nominal = Source**
- **Regional Archive = Source**

If both validations pass, the pipeline has successfully processed every generated telemetry record.

---

# Future Improvements

- Multi-node Kafka cluster
- Horizontal consumer scaling
- Avro or Protobuf serialization
- Prometheus & Grafana monitoring
- Kubernetes deployment
- Schema Registry integration

---

# Assignment Summary

This project demonstrates:

- High-throughput event streaming
- Apache Kafka producer and consumer development
- Parallel stream processing
- Resource-aware system design
- JVM performance tuning
- Docker-based deployment
- Automated build, execution, benchmarking, and verification

The final solution is fully automated, reproducible, and can be executed on any machine with Docker installed.
