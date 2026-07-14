package com.telemetry.consumer;

import com.telemetry.router.MessageRouter;
import com.telemetry.util.ProgressTimer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Service to consume raw telemetry records from Kafka source topic
 * and process/route them via MessageRouter.
 */
public class KafkaConsumerService implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(KafkaConsumerService.class);

    private final KafkaConsumer<String, String> consumer;
    private final String sourceTopic;
    private final Properties config;

    public KafkaConsumerService(String sourceTopic, Properties config) {
        this.sourceTopic = sourceTopic;
        this.config = config;
        this.consumer = new KafkaConsumer<>(config);
        log.info("KafkaConsumerService instantiated for topic '{}'", sourceTopic);
    }

    /**
     * Consumes exactly N records starting from the earliest offset in source topic.
     * Useful for validation runs.
     *
     * @param count  number of records to consume and route
     * @param router the router to delegate processing to
     * @param timer  optional ProgressTimer to track throughput
     */
    public void consumeExact(long count, MessageRouter router, ProgressTimer timer) {
        log.info("Starting consumeExact for count: {}", count);
        consumer.subscribe(Collections.singletonList(sourceTopic));
        
        // Force evaluation of partitions and seek to beginning
        pollAndSeekToBeginning();

        long processed = 0;
        if (timer != null) {
            timer.start(count);
        }

        boolean done = false;
        while (!done) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));
            if (records.isEmpty()) {
                // If we polled and got nothing, but haven't reached count, wait or break to prevent hanging
                log.warn("Polled empty batch. Processed {} of {}", processed, count);
                if (processed > 0) {
                    break; 
                }
                continue;
            }

            for (ConsumerRecord<String, String> rec : records) {
                router.route(rec.value());
                processed++;
                if (timer != null) {
                    timer.checkpoint(processed);
                }
                if (processed >= count) {
                    done = true;
                    break;
                }
            }
        }
        
        consumer.unsubscribe();
    }

    /**
     * Consumes all available messages currently in the source topic (from earliest to current end offsets).
     * Uses parallel worker threads (one per partition) for high-throughput concurrent routing.
     *
     * @param router the router to delegate processing to
     * @param timer  optional ProgressTimer to track throughput
     * @return the total number of records processed
     */
    public long consumeAll(MessageRouter router, ProgressTimer timer) {
        log.info("Starting parallel consumeAll. Querying partitions...");
        
        List<org.apache.kafka.common.PartitionInfo> partitionInfos;
        try {
            partitionInfos = consumer.partitionsFor(sourceTopic);
        } catch (Exception e) {
            log.error("Failed to query partitions for topic {}: {}", sourceTopic, e.getMessage());
            return 0;
        }
        
        if (partitionInfos == null || partitionInfos.isEmpty()) {
            log.warn("No partitions found for topic {}", sourceTopic);
            return 0;
        }

        int numThreads = partitionInfos.size();
        log.info("Topic '{}' has {} partitions. Spawning {} consumer threads.", sourceTopic, numThreads, numThreads);
        
        List<TopicPartition> partitions = partitionInfos.stream()
            .map(pi -> new TopicPartition(pi.topic(), pi.partition()))
            .collect(Collectors.toList());
            
        Map<TopicPartition, Long> endOffsets = consumer.endOffsets(partitions);
        Map<TopicPartition, Long> beginningOffsets = consumer.beginningOffsets(partitions);
        
        long totalRecordsToProcess = 0;
        for (TopicPartition tp : partitions) {
            long startPos = beginningOffsets.getOrDefault(tp, 0L);
            long endPos = endOffsets.getOrDefault(tp, 0L);
            long diff = endPos - startPos;
            if (diff > 0) {
                totalRecordsToProcess += diff;
            }
            log.info("Partition {}: startOffset={}, endOffset={}, toConsume={}", tp, startPos, endPos, diff);
        }

        log.info("Total records to route in parallel: {}", totalRecordsToProcess);

        if (totalRecordsToProcess == 0) {
            log.info("All partitions are empty. No records to route.");
            return 0;
        }

        if (timer != null) {
            timer.start(totalRecordsToProcess);
        }

        java.util.concurrent.atomic.AtomicLong totalProcessed = new java.util.concurrent.atomic.AtomicLong(0);
        Thread[] threads = new Thread[numThreads];

        for (int i = 0; i < numThreads; i++) {
            final int partitionId = i;
            threads[i] = new Thread(() -> {
                TopicPartition tp = new TopicPartition(sourceTopic, partitionId);
                long startOffset = beginningOffsets.getOrDefault(tp, 0L);
                long endOffset = endOffsets.getOrDefault(tp, 0L);
                long toProcess = endOffset - startOffset;

                if (toProcess <= 0) {
                    return;
                }

                // Create a fresh consumer instance for this thread
                try (KafkaConsumer<String, String> workerConsumer = new KafkaConsumer<>(config)) {
                    workerConsumer.assign(Collections.singletonList(tp));
                    workerConsumer.seek(tp, startOffset);

                    long processed = 0;
                    while (processed < toProcess) {
                        ConsumerRecords<String, String> records = workerConsumer.poll(Duration.ofMillis(200));
                        if (records.isEmpty()) {
                            // Verify position to avoid spinning endlessly
                            if (workerConsumer.position(tp) >= endOffset) {
                                break;
                            }
                            continue;
                        }

                        for (ConsumerRecord<String, String> rec : records) {
                            if (rec.offset() < endOffset) {
                                router.route(rec.value());
                                processed++;
                                long totalSoFar = totalProcessed.incrementAndGet();
                                if (timer != null) {
                                    synchronized (timer) {
                                        timer.checkpoint(totalSoFar);
                                    }
                                }
                            }
                        }
                    }
                    log.info("Thread for partition {} finished. Processed: {}", partitionId, processed);
                } catch (Exception e) {
                    log.error("Error in consumer thread for partition {}: {}", partitionId, e.getMessage(), e);
                }
            }, "consumer-worker-" + partitionId);
            threads[i].start();
        }

        // Wait for all threads to finish
        for (Thread t : threads) {
            try {
                t.join();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                log.warn("Main consumer thread interrupted while waiting for worker threads to finish.");
            }
        }

        return totalProcessed.get();
    }

    /**
     * Polls the consumer briefly to trigger partition assignment, then seeks to the beginning.
     *
     * @return the set of assigned partitions
     */
    private Set<TopicPartition> pollAndSeekToBeginning() {
        long start = System.currentTimeMillis();
        Set<TopicPartition> assignment = consumer.assignment();
        
        // Poll with short timeout until we get assignment (up to 5 seconds)
        while (assignment.isEmpty() && (System.currentTimeMillis() - start < 5000)) {
            consumer.poll(Duration.ofMillis(100));
            assignment = consumer.assignment();
        }

        if (!assignment.isEmpty()) {
            consumer.seekToBeginning(assignment);
            log.info("Sought to beginning for partitions: {}", assignment);
        } else {
            log.warn("Failed to get partition assignment after 5 seconds of polling.");
        }
        
        return assignment;
    }

    @Override
    public void close() {
        try {
            consumer.close();
            log.info("Kafka consumer closed successfully.");
        } catch (Exception e) {
            log.error("Error closing Kafka consumer: {}", e.getMessage());
        }
    }
}
