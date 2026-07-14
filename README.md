# IoT Telemetry Pipeline: High-Throughput Event Streaming System

A high-performance, resource-bounded IoT telemetry processing pipeline built with **Apache Kafka (KRaft)** and **Java 17**. 

This system generates, ingests, routes, and archives millions of telemetry records under a strict hardware budget of **2 GB RAM** and **4 CPU Cores**.

---

## 🚀 Key Achievements & Performance Metrics

| Metric | Target | Actual (Clean Docker Run) | Optimization Highlights |
| :--- | :--- | :--- | :--- |
| **Ingestion Rate** (Producer) | N/A | **811,872 records/sec** | LZ4 Compression, tuned Batching (64 KB, 20ms linger) |
| **Processing Rate** (Consumer/Router) | N/A | **479,906 records/sec** | Custom index-based CSV parser (0 regexes), shared producer |
| **JVM Garbage Collection** | Durability | **0 Full GCs / <8.5ms pauses** | G1 GC collector, minimal heap object allocations |
| **Algebraic Verification** | Strict Equality | **100% Pass** | `critical` + `nominal` == `source`; `archive` == `source` |
| **Memory Footprint** | <= 2.0 GB | **~1.1 GB (Peak JVM Heap)** | JRE Alpine base image, bounded heap `-Xms512m -Xmx1024m` |

---

## 📐 System Topology

The pipeline follows a real-time event-driven streaming topology with dynamic network lookup:

```text
       ┌────────────────────────┐
       │   Telemetry Generator  │
       └───────────┬────────────┘
                   │ (Produces 50M records)
                   ▼
       ┌────────────────────────┐
       │   Kafka Topic: source  │
       └───────────┬────────────┘
                   │
                   ▼
       ┌────────────────────────┐
       │     Kafka Consumer     │
       └───────────┬────────────┘
                   │ (Pulls batches)
                   ▼
       ┌────────────────────────┐
       │   Message Classifier   │
       └─────┬────────────┬─────┘
             │ (A-M)      │ (N-Z)
             ▼            ▼
       ┌───────────┐┌───────────┐
       │  Topic:   ││  Topic:   │
       │ critical  ││  nominal  │
       └─────┬─────┘└─────┬─────┘
             │            │
             └─────┬──────┘
                   │ (Always copied)
                   ▼
       ┌────────────────────────┐
       │ Topic: regional_archive│
       │   (Keyed by Region)    │
       └────────────────────────┘
```

---

## 🛠️ Technology Stack & Prerequisites

* **Core Runtime:** Java 17 (JRE)
* **Build System:** Maven 3.8+
* **Messaging Broker:** Apache Kafka (KRaft mode)
* **Containerization:** Docker Engine 20+ & Docker Compose v2+
* **OS Support:** Full automated support for **Linux/macOS** (`.sh` scripts) and **Windows** (`.ps1` PowerShell scripts)

---

## 📦 Project Structure

```
iot-telemetry-pipeline/
├── docker/                           # Kafka broker configuration files
├── docs/                             # Architecture and Performance docs
│   ├── Architecture.md               # Stream routing architecture design decisions
│   ├── Performance.md                # Milestone 4 memory and GC optimization analysis
│   └── Benchmark.md                  # Detailed metrics logs for repeatability verification
├── scripts/
│   ├── build.sh / build.ps1          # Clean, package Maven JAR, and assemble Docker image
│   ├── run.sh / run.ps1              # Boot cluster, auto-create topics, run producer & router
│   └── verify.sh / verify.ps1        # Queries Kafka broker offsets, runs PASS/FAIL checks
├── src/
│   ├── main/java/com/telemetry/
│   │   ├── model/
│   │   │   └── Telemetry.java        # Immutable domain model with fast index substring scanner
│   │   ├── generator/
│   │   │   └── TelemetryGenerator.java  # Thread-safe high-throughput dummy data generator
│   │   ├── producer/
│   │   │   ├── ProducerConfig.java   # baseline() and tuned() Kafka property configurations
│   │   │   └── KafkaProducerService.java  # Async Kafka sending wrapper with atomic offsets counters
│   │   ├── consumer/
│   │   │   ├── ConsumerConfig.java   # Optimizations for batch-polling consumers
│   │   │   └── KafkaConsumerService.java  # consumeExact and consumeAll loops
│   │   ├── router/
│   │   │   ├── MessageClassifier.java  # Stateless routing bounds (A-M vs N-Z)
│   │   │   └── MessageRouter.java    # Coordinates target writes and archival copy
│   │   ├── util/
│   │   │   ├── Constants.java        # Central configs supporting dynamic environment variables
│   │   │   ├── CsvValidator.java     # Telemetry schema rules evaluator
│   │   │   └── ProgressTimer.java    # Wall-clock timer with 5% increment logging
│   │   └── App.java                  # Main Orchestrator CLI Entrypoint
│   └── test/java/com/telemetry/
│       └── AppTest.java              # 24 unit tests covering parsing, routing, and boundaries
├── pom.xml                           # Maven dependencies
├── Dockerfile                        # Production JRE alpine image definition
└── docker-compose.yml                # Microservice composition map (Kafka, Topic-init, App)
```

---

## 🚀 Quick Start (Automated Execution)

The entire build, execution, and validation pipeline is completely automated. Run the commands below from the repository root:

### 1. Build the System
Compiles Java source files, runs tests, packages the dependency-loaded fat JAR, and compiles the local Docker image:
* **Linux/macOS:** `./scripts/build.sh`
* **Windows (PowerShell):** `.\scripts\build.ps1`

### 2. Run the Pipeline
Launches Kafka, waits for health checks, auto-creates all 4 topics with correct partitions, generates **50 million records**, and routes them:
* **Linux/macOS:** `./scripts/run.sh`
* **Windows (PowerShell):** `.\scripts\run.ps1`

### 3. Verify System State & Algebraic Checks
Queries active partition offsets from the broker, parses results, and executes core algebraic verifications:
* **Linux/macOS:** `./scripts/verify.sh`
* **Windows (PowerShell):** `.\scripts\verify.ps1`

*Expected Validation Output:*
```text
====================================================================
 Starting Telemetry Pipeline Count Verification (PowerShell)
====================================================================
Fetching topic offsets from Kafka broker...
----------------------------------------------------
Topic: source               | Total Records: 50,000,000
Topic: critical             | Total Records: 25,002,526
Topic: nominal              | Total Records: 24,997,474
Topic: regional_archive     | Total Records: 50,000,000
----------------------------------------------------
Extracted Counts:
  Source           : 50000000
  Critical         : 25002526
  Nominal          : 24997474
  Regional Archive : 50000000

====================================================================
 Check 1: Critical (25002526) + Nominal (24997474) == Source (50000000) ? [OK]
 Check 2: Regional Archive (50000000) == Source (50000000) ? [OK]

 FINAL RESULT: PASS
====================================================================
```

---

## ⚙️ Design & Performance Optimizations

### 1. CUSTOM INDEX-BASED CSV PARSER (Zero Regexes)
Using Java's standard `String.split()` or regular expressions triggers massive internal character-array copying and pattern matcher compile overhead. In [Telemetry.java](file:///c:/Users/PrudhviKarri/Documents/hsbc-test/iot-telemetry-pipeline/src/main/java/com/telemetry/model/Telemetry.java#L80-L115), we implemented a custom scanner using sequential `indexOf()` and substring markers. This reduced memory allocation rate by **60%** and improved routing throughput from **~251K rec/s** to **~480K rec/s** in Docker.

### 2. KAFKA PRODUCER BATCH TUNING
Default Kafka settings flush small batches immediately, overloading network queues. We tuned the producer config presets:
* `linger.ms = 20`: Accumulates data up to 20ms before flushing, dramatically increasing batch sizes.
* `batch.size = 65536` (64 KB): Expands maximum buffer space per network segment.
* `compression.type = lz4`: Ultra-fast text compression minimizing network overhead.

### 3. JVM GARBAGE COLLECTION BOUNDS
To ensure the JVM never exceeds the 2 GB memory ceiling:
* Implemented the **G1 Garbage Collector** (`-XX:+UseG1GC`) optimized for low-pause, high-throughput memory compaction.
* Set initial Heap at **512 MB** (`-Xms512m`) and max Heap at **1024 MB** (`-Xmx1024m`) to prevent OS-level OOM kills while keeping enough operating memory for Kafka container services.

---

## 🔒 Security & Durability Considerations
* **Non-Root Privileges:** The application runs inside Docker under a dedicated, unprivileged user/group (`appuser:appgroup`).
* **Topic Creation Ordering:** The application container blocks until the topic initializer (`kafka-init`) returns code `0`, ensuring partition counts (3 partitions per topic) conform strictly to requirements before ingestion begins.
* **Network Isolation:** Kafka internal listeners are mapped strictly inside the docker subnet, exposing only the bootstrap interface to external hosts.
