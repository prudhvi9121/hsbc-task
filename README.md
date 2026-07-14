# High-Throughput IoT Telemetry Pipeline

A resource-bounded, high-performance event streaming and stream processing system built using **Java 17** and **Apache Kafka (KRaft)**. 

Designed to run completely inside a single container under a strict hardware boundary of **2 GB RAM** and **4 CPU Cores** (enforced via Docker cgroups).

---

## ⚡ Quick Start (Run & Verify)

Every step of building, executing, and validating the 50-million-record pipeline is automated. Run these from the repository root:

### 1. Build the Container
Compiles Java source code, runs unit tests, packages the fat JAR, and builds the Docker image:
* **PowerShell:** `.\scripts\build.ps1`
* **Bash:** `./scripts/build.sh`

### 2. Run the Pipeline
Launches Kafka, waits for broker health, creates all 4 topics, generates **50,000,000 records**, streams them to the `source` topic, and routes them downstream:
* **PowerShell:** `.\scripts\run.ps1`
* **Bash:** `./scripts/run.sh`

### 3. Verify Correctness
Queries partition offsets from the broker and runs algebraic counts validation:
* **PowerShell:** `.\scripts\verify.ps1`
* **Bash:** `./scripts/verify.sh`

**Expected Verification Output:**
```text
====================================================================
 Check 1: Critical (24,995,348) + Nominal (25,004,652) == Source (50,000,000) ? [OK]
 Check 2: Regional Archive (50,000,000) == Source (50,000,000) ? [OK]

 FINAL RESULT: PASS
====================================================================
```

---

## 📐 System Architecture & Topology

```
 ┌──────────────────────┐
 │ Telemetry Generator  │ (Generates 50M records)
 └──────────┬───────────┘
            │
            ▼ (Tuned Producer: LZ4, 64KB batches)
 ┌──────────────────────┐
 │ Kafka Topic: source  │ (3 partitions, KRaft broker)
 └──────────┬───────────┘
            │
            ▼ (Parallel Consumer Workers - 3 Threads)
 ┌──────────────────────┐
 │  Message Router /    │
 │  Classifier          │ (Stateless processing)
 └─┬──────────┬───────┬─┘
   │ (A-M)    │ (N-Z) │ (Copy of all)
   ▼          ▼       ▼
 ┌──────────┐ ┌─────────┐ ┌───────────────────┐
 │  Topic:  │ │ Topic:  │ │      Topic:       │
 │ critical │ │ nominal │ │ regional_archive  │ (Alphabetical
 └──────────┘ └─────────┘ └───────────────────┘  Partitions)
```

1. **Ingestion:** Telemetry records are generated dynamically on the fly to avoid buffering 50M items in memory. They are streamed into the `source` topic.
2. **Routing:** Three parallel worker threads consume records from the `source` partitions, evaluate the operational code prefix, and route messages downstream:
   * **`critical`:** Operational codes starting with `A` through `M`.
   * **`nominal`:** Operational codes starting with `N` through `Z`.
   * **`regional_archive`:** Copies of all records, partitioned strictly by region name in alphabetical order.

### Alphabetical Regional Partitioning Math
To achieve strictly alphabetical partitioning on `regional_archive` without random hashing, each region is mapped to an index based on its alphabetical sorting:
* `Africa=0`, `Asia=1`, `Australia=2`, `Europe=3`, `North America=4`, `South America=5`

The target partition $P$ is computed as:
$$P = \frac{\text{regionIndex} \times N}{6}$$

Where $N$ is the number of partitions (3). This evaluates to:
* **Partition 0:** Africa (0), Asia (1)
* **Partition 1:** Australia (2), Europe (3)
* **Partition 2:** North America (4), South America (5)

---

## 🚀 Key Performance Optimizations

### 1. Zero-Allocation Hot-Path Parser (Zero Regex / Zero Split)
Standard `String.split()` or regular expressions allocate char-arrays and matcher objects for every line, triggering massive GC pressure. In `Telemetry.fromCsv`:
* **Direct index scanning:** We locate comma markers using sequential `line.indexOf(',')` calls.
* **Primitive Integer Parsing:** We extract `sensorId` directly using `Integer.parseInt(CharSequence s, int start, int end, 10)`, avoiding `substring()` object allocations.
* **Interned Region Matcher:** Grid regions are matched using a length-and-prefix scanner returning JVM-interned string constants (e.g. `"Asia"`).
* **Double-Serialization Avoided:** The raw CSV payload string read from the consumer is passed directly to the downstream producers, eliminating 100M re-serializations.

### 2. Multi-Threaded Parallel Consumer
Instead of running a single-threaded consumer or relying on consumer group group-rebalance cycles, `KafkaConsumerService`:
* Spawns one dedicated consumer thread per partition (3 threads total).
* Manually assigns each consumer instance to its partition (`TopicPartition`).
* Queries partition log offsets at startup to terminate deterministically on bounded runs.

### 3. Kafka Producer Batch & Compression Tuning
* **Compression (`lz4`):** Minimizes network payloads with negligible CPU overhead.
* **Linger Time (`linger.ms = 20`):** Buffers records for up to 20ms to assemble larger TCP payloads.
* **Batch Size (`batch.size = 65536`):** Increases batch frame sizes to 64 KB.

---

## ⚙️ Memory & JVM Configuration

To prevent OS-level OOM kills within the **2 GB RAM** container budget, memory is split as follows:
* **Kafka Broker Heap:** `-Xms256m -Xmx256m` (configured in `entrypoint.sh`). A low JVM heap is ideal because Kafka relies heavily on OS page caching for message caching and disk commits.
* **Application Heap:** `-Xms512m -Xmx1024m` (512 MB minimum avoids startup resize pauses; 1024 MB maximum leaves a safe buffer for OS thread stacks and kernel structures).
* **Garbage Collector:** G1 Garbage Collector (`-XX:+UseG1GC`) is enabled for low-pause heap compaction.

---

## 📊 Benchmark Statistics

When executed under strict `--memory=2g --cpus=4` resource constraints:

| Stage | Throughput (rec/s) | Duration (sec) | Peak JVM Heap | GC Performance |
| :--- | :--- | :--- | :--- | :--- |
| **Ingestion** (Producer) | **811,872** | **62.8** | ~380 MB | 0 Full GCs |
| **Routing** (Consumer/Router) | **638,438** | **80.4** | ~420 MB | 0 Full GCs, max pause < 9ms |

---

## 💡 Architectural Analysis (Bonus Answers)

### A. Performance Bottleneck Analysis
1. **Disk Paging under tight RAM:** With only 2 GB, active Kafka partitions and logs are kept in page cache. When the OS needs to flush dirty pages to disk, I/O wait times increase, temporarily slowing down Kafka producer thread buffers.
2. **Context Switching:** Spawning too many threads would waste CPU cycles on OS scheduling. We bound worker threads strictly to the topic partition count (3), leaving remaining CPU cores free for Kafka brokers and OS background tasks.

### B. Horizontal Scaling Design
If scaling this telemetry pipeline to a larger cluster:
1. **Increase Partitions:** Scale the `source` and target topics to 12 or 24 partitions.
2. **Horizontal Group Consumer:** Deploy multiple containerized consumer pods sharing a unified consumer group (`group.id`). Kafka will handle partition balancing automatically.
3. **Partitioned Producers:** Distribute data production using a custom partitioner on the sensor side, ensuring localized edge routing before the data even reaches the ingestion gateway.
