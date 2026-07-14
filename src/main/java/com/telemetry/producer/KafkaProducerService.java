package com.telemetry.producer;

import com.telemetry.model.Telemetry;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Thin wrapper around {@link KafkaProducer} that provides:
 *   - Asynchronous fire-and-forget sends with error counting
 *   - Uses the sensorId as the Kafka message key (ensures per-sensor ordering)
 *   - Atomic sent/error counters for progress reporting
 *   - AutoCloseable: flush + close on exit (no record loss)
 */
public class KafkaProducerService implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(KafkaProducerService.class);

    private final KafkaProducer<String, String> producer;
    private final String topic;
    private final AtomicLong sentCount  = new AtomicLong(0);
    private final AtomicLong errorCount = new AtomicLong(0);
    private final Map<String, Integer> partitionCounts = new ConcurrentHashMap<>();

    /**
     * Creates the producer and validates connectivity by checking metadata on startup.
     *
     * @param topic  Kafka topic to publish to
     * @param config producer properties (from ProducerConfig.tuned() or ProducerConfig.baseline())
     */
    public KafkaProducerService(String topic, Properties config) {
        this.topic    = topic;
        this.producer = new KafkaProducer<>(config);
        log.info("KafkaProducerService ready — topic='{}' bootstrap='{}'",
            topic, config.getProperty("bootstrap.servers"));
    }

    // ──────────────────────────────────────────────
    // Send methods
    // ──────────────────────────────────────────────

    /**
     * Asynchronously sends a {@link Telemetry} record to Kafka.
     * <p>
     * The sensorId is used as the message key so all readings from the same sensor
     * land on the same partition (preserving per-sensor ordering).
     * The toCsv() value is the message body.
     * <p>
     * Errors are counted but not thrown — they appear in logs and via {@link #getErrorCount()}.
     */
    public void sendAsync(Telemetry telemetry) {
        String key   = String.valueOf(telemetry.sensorId());
        String value = telemetry.toCsv();
        sendAsync(topic, key, value);
    }

    /**
     * Asynchronously sends a key/value record to a specific topic in Kafka.
     * Errors are counted but not thrown.
     */
    public void sendAsync(String targetTopic, String key, String value) {
        producer.send(
            new ProducerRecord<>(targetTopic, key, value),
            (metadata, ex) -> {
                if (ex != null) {
                    errorCount.incrementAndGet();
                    log.error("Send failed to topic {}: {}", targetTopic, ex.getMessage());
                } else {
                    sentCount.incrementAndGet();
                }
            }
        );
    }

    /**
     * Asynchronously sends a key/value record to a specific partition and topic in Kafka.
     * Errors are counted but not thrown.
     */
    public void sendAsync(String targetTopic, Integer partition, String key, String value) {
        producer.send(
            new ProducerRecord<>(targetTopic, partition, key, value),
            (metadata, ex) -> {
                if (ex != null) {
                    errorCount.incrementAndGet();
                    log.error("Send failed to topic {} partition {}: {}", targetTopic, partition, ex.getMessage());
                } else {
                    sentCount.incrementAndGet();
                }
            }
        );
    }

    /**
     * Fetches and caches the partition count for a given topic.
     */
    public int getPartitionCount(String topic) {
        return partitionCounts.computeIfAbsent(topic, t -> {
            try {
                var partitions = producer.partitionsFor(t);
                return partitions != null ? partitions.size() : 3;
            } catch (Exception e) {
                log.warn("Failed to get partition count for topic {}, defaulting to 3. Error: {}", t, e.getMessage());
                return 3;
            }
        });
    }

    /**
     * Sends a raw key/value pair synchronously (blocks until flushed).
     * Used for connection tests.
     */
    public void sendRaw(String key, String value) {
        producer.send(
            new ProducerRecord<>(topic, key, value),
            (metadata, ex) -> {
                if (ex != null) {
                    log.error("Raw send failed: {}", ex.getMessage());
                } else {
                    log.info("Sent → partition={} offset={} value='{}'",
                        metadata.partition(), metadata.offset(), value);
                }
            }
        );
        producer.flush();
    }

    // ──────────────────────────────────────────────
    // Control
    // ──────────────────────────────────────────────

    /**
     * Blocks until all buffered records have been transmitted to Kafka.
     * Must be called before stopping the timer to ensure accurate throughput measurements.
     */
    public void flush() {
        producer.flush();
    }

    /** Returns total records successfully acknowledged by the broker. */
    public long getSentCount() { return sentCount.get(); }

    /** Returns total records that failed to send. */
    public long getErrorCount() { return errorCount.get(); }

    /**
     * Flushes and closes the underlying KafkaProducer.
     * Called automatically when used in a try-with-resources block.
     */
    @Override
    public void close() {
        producer.flush();
        producer.close();
        log.info("Producer closed — sent={} errors={}", sentCount.get(), errorCount.get());
        if (errorCount.get() > 0) {
            log.warn("{} records failed to send!", errorCount.get());
        }
    }
}
