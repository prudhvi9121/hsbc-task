package com.telemetry.router;

import com.telemetry.model.Telemetry;
import com.telemetry.producer.KafkaProducerService;
import com.telemetry.util.Constants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Optional;
import java.util.concurrent.atomic.AtomicLong;

/**
 * MessageRouter handles parsing raw telemetry CSV strings, classifying them
 * into critical or nominal streams, publishing them, and archiving them.
 */
public class MessageRouter {

    private static final Logger log = LoggerFactory.getLogger(MessageRouter.class);

    private final KafkaProducerService producerService;
    private final AtomicLong criticalCount = new AtomicLong(0);
    private final AtomicLong nominalCount  = new AtomicLong(0);
    private final AtomicLong archiveCount  = new AtomicLong(0);
    private final AtomicLong parsedErrorCount = new AtomicLong(0);

    public MessageRouter(KafkaProducerService producerService) {
        this.producerService = producerService;
    }

    /**
     * Routes a raw CSV record.
     *
     * @param rawCsv the raw telemetry message
     */
    public void route(String rawCsv) {
        Optional<Telemetry> optTelemetry = Telemetry.parseSafe(rawCsv);
        if (optTelemetry.isEmpty()) {
            parsedErrorCount.incrementAndGet();
            log.error("Failed to parse telemetry record: {}", rawCsv);
            return;
        }

        Telemetry telemetry = optTelemetry.get();
        
        // Extract key from rawCsv directly to avoid String.valueOf() formatting overhead
        int commaIdx = rawCsv.indexOf(',');
        String key = (commaIdx != -1) ? rawCsv.substring(0, commaIdx) : String.valueOf(telemetry.sensorId());

        // 1. Classify and send to either critical or nominal
        boolean isCrit = MessageClassifier.isCritical(telemetry);
        String targetTopic = isCrit ? Constants.TOPIC_CRITICAL : Constants.TOPIC_NOMINAL;
        
        producerService.sendAsync(targetTopic, key, rawCsv);
        if (isCrit) {
            criticalCount.incrementAndGet();
        } else {
            nominalCount.incrementAndGet();
        }

        // 2. Always copy to regional_archive. Partition is assigned strictly in alphabetical order of regions.
        int regionIndex = MessageClassifier.getRegionIndex(telemetry.gridRegion());
        int numPartitions = producerService.getPartitionCount(Constants.TOPIC_ARCHIVE);
        int partition = (regionIndex * numPartitions) / 6;

        producerService.sendAsync(Constants.TOPIC_ARCHIVE, partition, telemetry.gridRegion(), rawCsv);
        archiveCount.incrementAndGet();
    }

    /** Resets routing counters to zero. */
    public void resetCounters() {
        criticalCount.set(0);
        nominalCount.set(0);
        archiveCount.set(0);
        parsedErrorCount.set(0);
    }

    // Getters for counts
    public long getCriticalCount() { return criticalCount.get(); }
    public long getNominalCount() { return nominalCount.get(); }
    public long getArchiveCount() { return archiveCount.get(); }
    public long getParsedErrorCount() { return parsedErrorCount.get(); }

    /** Prints a verification report showing counts and relationships. */
    public void printReport(long expectedInput) {
        long c = criticalCount.get();
        long n = nominalCount.get();
        long a = archiveCount.get();
        long sum = c + n;

        System.out.println("+--------------------------------------------------------------+");
        System.out.println("|                Message Routing Verification Report           |");
        System.out.println("+--------------------------------------------------------------+");
        System.out.printf ("|  Expected Input Processed  : %,30d  |%n", expectedInput);
        System.out.printf ("|  Routed Critical           : %,30d  |%n", c);
        System.out.printf ("|  Routed Nominal            : %,30d  |%n", n);
        System.out.printf ("|  Routed Regional Archive   : %,30d  |%n", a);
        System.out.printf ("|  Sum (Critical + Nominal)  : %,30d  |%n", sum);
        System.out.printf ("|  Parse Errors              : %,30d  |%n", parsedErrorCount.get());
        System.out.println("+--------------------------------------------------------------+");

        boolean sumOk = (sum + parsedErrorCount.get()) == expectedInput;
        boolean archiveOk = (a + parsedErrorCount.get()) == expectedInput;
        
        System.out.printf ("|  Check: Critical + Nominal == Source?  -> %-19s  |%n", sumOk ? "PASS" : "FAIL");
        System.out.printf ("|  Check: Archive == Source?             -> %-19s  |%n", archiveOk ? "PASS" : "FAIL");
        System.out.println("+--------------------------------------------------------------+");
    }
}
