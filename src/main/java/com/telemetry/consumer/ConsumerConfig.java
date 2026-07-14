package com.telemetry.consumer;

import com.telemetry.util.Constants;
import java.util.Properties;

/**
 * Presets for Kafka Consumer configuration.
 */
public final class ConsumerConfig {

    private ConsumerConfig() {}

    /**
     * Creates default tuned properties for high-throughput consuming.
     *
     * @param groupId the consumer group ID
     * @return Properties object with configurations set
     */
    public static Properties defaultSettings(String groupId) {
        Properties p = new Properties();
        p.put("bootstrap.servers", Constants.BOOTSTRAP_SERVERS);
        p.put("group.id", groupId);
        p.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        p.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        
        // Start from earliest offset when no committed offset exists
        p.put("auto.offset.reset", "earliest");
        
        // Auto-commit offsets periodically (simple and standard for this processing step)
        p.put("enable.auto.commit", "true");
        p.put("auto.commit.interval.ms", "5000");

        // High-throughput consumer optimizations
        p.put("max.poll.records", "10000");      // Process up to 10k records in one poll batch
        p.put("fetch.min.bytes", "1048576");      // Wait for at least 1MB of data (increases batch size)
        p.put("fetch.max.wait.ms", "500");       // Max timeout to wait if min bytes not met

        return p;
    }
}
