package com.telemetry.producer;

import com.telemetry.util.Constants;

import java.util.Properties;

/**
 * Kafka producer property presets for the IoT Telemetry Pipeline.
 *
 * Two presets are available:
 *   baseline() — vanilla defaults, used to establish a performance baseline
 *   tuned()    — high-throughput optimised settings
 *
 * Tuning rationale (each property commented in-line below):
 *
 *   acks = 1
 *     Leader-only acknowledgement.  The leader writes the record and ACKs immediately
 *     without waiting for in-sync replicas.  For a single-broker dev cluster this is
 *     equivalent to acks=all; in production it gives maximum throughput while keeping
 *     the record durable on the leader.
 *
 *   linger.ms = 20
 *     The producer waits up to 20 ms to accumulate more records before sending a batch.
 *     A non-zero linger dramatically increases average batch size at the cost of at most
 *     20 ms of additional end-to-end latency — an excellent trade-off for bulk ingestion.
 *
 *   batch.size = 65536  (64 KB)
 *     Maximum uncompressed bytes per batch.  Larger batches mean fewer network round-trips.
 *     The default (16 KB) is conservative; 64 KB is a common production sweet-spot.
 *
 *   buffer.memory = 67108864  (64 MB)
 *     Total memory the producer can use to buffer records waiting to be sent.  A generous
 *     buffer prevents back-pressure stalls when the network is momentarily slower than
 *     the generation rate.
 *
 *   compression.type = lz4
 *     LZ4 achieves excellent compression ratios on repetitive CSV text while running at
 *     GB/s speeds — it adds negligible CPU overhead.  This reduces both network bandwidth
 *     and broker-side storage for the source topic.
 *
 *   max.in.flight.requests.per.connection = 5
 *     Allows up to 5 batches to be in-flight simultaneously on a single TCP connection,
 *     effectively pipelining the network I/O and saturating available bandwidth.
 *     (Safe at this value because we are not requiring strict ordering with acks != -1
 *      across retries, but retries=3 keeps reliability intact.)
 *
 *   retries = 3
 *     Automatically retries up to 3 times on transient leader elections or network blips
 *     before surfacing an error to the application callback.
 */
public final class ProducerConfig {

    private ProducerConfig() {}

    private static Properties base() {
        Properties p = new Properties();
        p.put("bootstrap.servers", Constants.BOOTSTRAP_SERVERS);
        p.put("key.serializer",   "org.apache.kafka.common.serialization.StringSerializer");
        p.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        return p;
    }

    /**
     * Baseline (untuned) configuration — Kafka defaults for linger, batch size,
     * and compression.  Used to establish a before-optimisation benchmark.
     */
    public static Properties baseline() {
        Properties p = base();
        p.put("acks", "1");
        // linger.ms = 0 (default) — send immediately, no batching wait
        // batch.size = 16384 (default)
        // compression.type = none (default)
        return p;
    }

    /**
     * High-throughput tuned configuration.
     * See class-level Javadoc for rationale of each property.
     */
    public static Properties tuned() {
        Properties p = base();
        p.put("acks",                                    "1");
        p.put("linger.ms",                               "20");
        p.put("batch.size",                              "65536");
        p.put("buffer.memory",                           "67108864");
        p.put("compression.type",                        "lz4");
        p.put("max.in.flight.requests.per.connection",   "5");
        p.put("retries",                                 "3");
        return p;
    }
}
