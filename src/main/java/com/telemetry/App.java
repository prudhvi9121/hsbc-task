package com.telemetry;

import com.telemetry.generator.TelemetryGenerator;
import com.telemetry.model.Telemetry;
import com.telemetry.producer.KafkaProducerService;
import com.telemetry.producer.ProducerConfig;
import com.telemetry.consumer.ConsumerConfig;
import com.telemetry.consumer.KafkaConsumerService;
import com.telemetry.router.MessageRouter;
import com.telemetry.util.Constants;
import com.telemetry.util.CsvValidator;
import com.telemetry.util.ProgressTimer;
import com.telemetry.util.Timer;

import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.stream.Collectors;

/**
 * Main entry point and CLI orchestrator for the IoT Telemetry Pipeline.
 *
 * <p>Exposes a set of named commands that can be composed to run individual
 * stages of the pipeline or the complete end-to-end flow in a single invocation.
 * Each command is self-contained and reports wall-clock timing on completion.
 *
 * <p>Run {@code java -jar app.jar help} or pass no arguments to print usage.
 */
public class App {

    public static void main(String[] args) throws Exception {
        String command = (args.length > 0) ? args[0].toLowerCase() : "help";
        long   count   = (args.length > 1) ? Long.parseLong(args[1]) : Constants.COUNT_SMALL;

        switch (command) {
            case "print"        -> samplePrint();
            case "validate"     -> sampleValidate();
            case "connect"      -> brokerConnectTest();
            case "produce"      -> produce(count);
            case "benchmark"    -> benchmark();
            case "fullrun"      -> fullRun();
            case "route"        -> route(count);
            case "routeall"     -> routeAll();
            case "countoffsets" -> countOffsets();
            case "all"          -> runAll();
            default             -> printUsage();
        }
    }

    // ──────────────────────────────────────────────────────────────────
    // Sample generation — prints a small set of records to stdout so
    // operators can visually confirm the CSV schema before a full run.
    // ──────────────────────────────────────────────────────────────────
    static void samplePrint() {
        banner("Generate & Print " + Constants.COUNT_PRINT + " Sample Records");
        TelemetryGenerator gen = new TelemetryGenerator();
        for (int i = 0; i < Constants.COUNT_PRINT; i++) {
            System.out.println(gen.generateTelemetry().toCsv());
        }
        System.out.println();
        System.out.println("─── Done (" + Constants.COUNT_PRINT + " records printed) ───");
    }

    // ──────────────────────────────────────────────────────────────────
    // Schema validation — generates a small CSV file on disk and runs
    // CsvValidator against it to ensure every field conforms to the
    // expected schema before large-scale ingestion begins.
    // ──────────────────────────────────────────────────────────────────
    static void sampleValidate() throws IOException {
        banner("Generate " + Constants.COUNT_VALIDATE + " Records → sample.csv → Validate Schema");
        TelemetryGenerator gen = new TelemetryGenerator();
        String filePath = "sample.csv";

        try (BufferedWriter bw = new BufferedWriter(new FileWriter(filePath))) {
            for (int i = 0; i < Constants.COUNT_VALIDATE; i++) {
                bw.write(gen.generateTelemetry().toCsv());
                bw.newLine();
            }
        }
        System.out.printf("Wrote %,d records to %s%n", Constants.COUNT_VALIDATE, filePath);

        boolean ok = CsvValidator.validate(filePath);
        if (!ok) {
            throw new RuntimeException("Schema validation FAILED — see errors above.");
        }
    }

    // ──────────────────────────────────────────────────────────────────
    // Broker connectivity test — sends a single probe message to the
    // source topic and confirms the broker acknowledged it. Use this
    // to verify network access and topic existence before a full run.
    // ──────────────────────────────────────────────────────────────────
    static void brokerConnectTest() {
        banner("Kafka Broker Connectivity Test");
        try (KafkaProducerService svc =
                 new KafkaProducerService(Constants.TOPIC_SOURCE, ProducerConfig.tuned())) {
            svc.sendRaw("connection-test", "Hello Kafka");
        }
        System.out.println();
        System.out.println("Message sent. Verify with:");
        System.out.println("""
            docker exec kafka /opt/kafka/bin/kafka-console-consumer.sh \\
              --topic source --from-beginning \\
              --bootstrap-server localhost:9092 --max-messages 5
            """);
    }

    // ──────────────────────────────────────────────────────────────────
    // Ingestion — generates N telemetry records on the fly and streams
    // them directly to the source topic using the high-throughput
    // producer configuration (LZ4 compression, 64 KB batches).
    // Records are generated and discarded immediately to keep memory
    // usage constant regardless of batch size.
    // ──────────────────────────────────────────────────────────────────
    static void produce(long count) {
        banner(String.format("Ingesting %,d Records to Source Topic", count));
        TelemetryGenerator gen   = new TelemetryGenerator();
        ProgressTimer      timer = new ProgressTimer();

        try (KafkaProducerService svc =
                 new KafkaProducerService(Constants.TOPIC_SOURCE, ProducerConfig.tuned())) {
            timer.start(count);
            for (long i = 0; i < count; i++) {
                svc.sendAsync(gen.generateTelemetry());
                timer.checkpoint(i + 1);
            }
            // Flush ensures all buffered records reach the broker before
            // the timer stops, giving accurate end-to-end throughput figures.
            svc.flush();
        }
        timer.stop(count);
        printVerifyHint(count);
    }

    // ──────────────────────────────────────────────────────────────────
    // Throughput benchmark — runs the same workload against the default
    // (untuned) producer and the optimised producer back to back and
    // prints a side-by-side comparison. Useful for verifying that tuning
    // changes (batching, compression, linger) are having the expected
    // effect on a given host/network combination.
    // ──────────────────────────────────────────────────────────────────
    static void benchmark() {
        banner("Producer Throughput Benchmark — Baseline vs Tuned (" +
               String.format("%,d", Constants.COUNT_SMALL) + " records each)");

        TelemetryGenerator gen = new TelemetryGenerator();

        // ── Baseline (vanilla Kafka defaults) ───────────────────────
        System.out.println("── Baseline (no tuning) ──");
        ProgressTimer baseTimer = new ProgressTimer();
        try (KafkaProducerService svc =
                 new KafkaProducerService(Constants.TOPIC_SOURCE, ProducerConfig.baseline())) {
            baseTimer.start(0);   // timing only, progress printing suppressed
            for (long i = 0; i < Constants.COUNT_SMALL; i++) {
                svc.sendAsync(gen.generateTelemetry());
            }
            svc.flush();
        }
        long baseMs = baseTimer.stop(Constants.COUNT_SMALL);

        System.out.println();

        // ── Tuned (LZ4, 64 KB batch, 20 ms linger) ──────────────────
        System.out.println("── Tuned (lz4, 64KB batch, 20ms linger) ──");
        ProgressTimer tunedTimer = new ProgressTimer();
        try (KafkaProducerService svc =
                 new KafkaProducerService(Constants.TOPIC_SOURCE, ProducerConfig.tuned())) {
            tunedTimer.start(0);
            for (long i = 0; i < Constants.COUNT_SMALL; i++) {
                svc.sendAsync(gen.generateTelemetry());
            }
            svc.flush();
        }
        long tunedMs = tunedTimer.stop(Constants.COUNT_SMALL);

        // ── Summary ──────────────────────────────────────────────────
        System.out.println();
        double speedup = baseMs > 0 ? (double) baseMs / tunedMs : 1.0;
        System.out.printf("  Baseline : %,.3f sec%n", baseMs  / 1000.0);
        System.out.printf("  Tuned    : %,.3f sec%n", tunedMs / 1000.0);
        System.out.printf("  Speedup  : %.2f×%n",     speedup);
        System.out.println();
    }

    // ──────────────────────────────────────────────────────────────────
    // Full ingestion run — produces the complete 50-million-record
    // dataset to the source topic with 5%-interval progress reporting.
    // This is the primary data-loading step before stream routing.
    // ──────────────────────────────────────────────────────────────────
    static void fullRun() {
        banner(String.format("Full Ingestion: %,d Records", Constants.COUNT_FULL));
        System.out.println("  Progress reported every 5% (~2.5M records)");
        System.out.println();

        TelemetryGenerator gen   = new TelemetryGenerator();
        ProgressTimer      timer = new ProgressTimer();

        try (KafkaProducerService svc =
                 new KafkaProducerService(Constants.TOPIC_SOURCE, ProducerConfig.tuned())) {
            timer.start(Constants.COUNT_FULL);
            for (long i = 0; i < Constants.COUNT_FULL; i++) {
                svc.sendAsync(gen.generateTelemetry());
                timer.checkpoint(i + 1);
            }
            svc.flush();
        }

        timer.stop(Constants.COUNT_FULL);
        printVerifyHint(Constants.COUNT_FULL);
    }

    // ──────────────────────────────────────────────────────────────────
    // Bounded routing run — produces exactly N fresh records then
    // immediately consumes and routes those same N records. Designed
    // for integration testing where a deterministic, bounded dataset
    // is needed to verify routing correctness before a full-scale run.
    // ──────────────────────────────────────────────────────────────────
    static void route(long count) {
        banner(String.format("Route %,d Records (Produce → Consume → Classify → Route)", count));

        // Step 1: Produce fresh records to the source topic.
        System.out.println("Producing " + count + " records to source topic...");
        produce(count);
        System.out.println("Producer flushed. Starting consumer and router...");

        // Step 2: Consume exactly N records starting from the earliest
        // available offset and route each one to its target topic.
        // A unique consumer group ID prevents interference with other
        // concurrent or past runs.
        Properties consumerProps = ConsumerConfig.defaultSettings("route-group-" + System.currentTimeMillis());
        Properties producerProps = ProducerConfig.tuned();

        ProgressTimer timer = new ProgressTimer();

        try (KafkaConsumerService consumerSvc = new KafkaConsumerService(Constants.TOPIC_SOURCE, consumerProps);
             KafkaProducerService producerSvc = new KafkaProducerService(Constants.TOPIC_SOURCE, producerProps)) {

            MessageRouter router = new MessageRouter(producerSvc);

            System.out.println("\nRouting records from source to target topics...");
            consumerSvc.consumeExact(count, router, timer);

            producerSvc.flush();
            System.out.println("\nRouting complete. Verification report:\n");
            router.printReport(count);
        }
    }

    // ──────────────────────────────────────────────────────────────────
    // Full routing run — consumes every record currently available in
    // the source topic from the earliest offset to the current broker
    // end offset, routing each record to its downstream topic in
    // parallel. Three consumer threads (one per partition) are spawned
    // so all CPU cores are active simultaneously during routing.
    // ──────────────────────────────────────────────────────────────────
    static void routeAll() {
        banner("Route All Available Source Records");

        Properties consumerProps = ConsumerConfig.defaultSettings("route-all-group");
        Properties producerProps = ProducerConfig.tuned();

        ProgressTimer timer       = new ProgressTimer();
        Timer         routeTimer  = new Timer();

        try (KafkaConsumerService consumerSvc = new KafkaConsumerService(Constants.TOPIC_SOURCE, consumerProps);
             KafkaProducerService producerSvc = new KafkaProducerService(Constants.TOPIC_SOURCE, producerProps)) {

            MessageRouter router = new MessageRouter(producerSvc);

            routeTimer.start();
            long processed = consumerSvc.consumeAll(router, timer);
            producerSvc.flush();
            routeTimer.stop();

            System.out.println("\nAll source records routed. Verification report:\n");
            router.printReport(processed);

            if (processed > 0) {
                routeTimer.printSummary("Routing Throughput", processed);
            }
        }
    }

    // ──────────────────────────────────────────────────────────────────
    // Offset inspection — queries the broker's latest offset for every
    // partition of each pipeline topic and prints the per-partition and
    // aggregate counts. Use this after a routing run to verify that
    // critical + nominal == source and archive == source.
    // ──────────────────────────────────────────────────────────────────
    static void countOffsets() {
        banner("Topic Offset Report");

        Properties props = ConsumerConfig.defaultSettings("offset-verifier-" + System.currentTimeMillis());

        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props)) {
            String[] topics = {
                Constants.TOPIC_SOURCE,
                Constants.TOPIC_CRITICAL,
                Constants.TOPIC_NOMINAL,
                Constants.TOPIC_ARCHIVE
            };

            System.out.println("Querying latest offsets from Kafka broker...\n");

            for (String topic : topics) {
                List<PartitionInfo> partitionInfos = consumer.partitionsFor(topic);
                if (partitionInfos == null || partitionInfos.isEmpty()) {
                    System.out.printf("Topic: %-20s | (topic not found or has no partitions)%n", topic);
                    continue;
                }

                List<TopicPartition> partitions = partitionInfos.stream()
                    .map(pi -> new TopicPartition(pi.topic(), pi.partition()))
                    .collect(Collectors.toList());

                Map<TopicPartition, Long> endOffsets = consumer.endOffsets(partitions);
                long total = endOffsets.values().stream().mapToLong(Long::longValue).sum();

                System.out.printf("Topic: %-20s | Total Records: %,d%n", topic, total);
                for (TopicPartition tp : partitions) {
                    System.out.printf("  └─ Partition %d offset: %,d%n", tp.partition(), endOffsets.get(tp));
                }
                System.out.println();
            }
        }
    }

    // ──────────────────────────────────────────────────────────────────
    // End-to-end pipeline — runs every command in the correct order:
    // schema preview → validation → connectivity check → small
    // ingestion → benchmark → medium ingestion → full 50M ingestion
    // → bounded routing test → full routing run.
    // ──────────────────────────────────────────────────────────────────
    static void runAll() throws IOException {
        samplePrint();                              System.out.println();
        sampleValidate();                           System.out.println();
        brokerConnectTest();                        System.out.println();
        produce(Constants.COUNT_SMALL);             System.out.println();
        benchmark();                                System.out.println();
        produce(Constants.COUNT_MEDIUM);            System.out.println();
        fullRun();                                  System.out.println();
        route(Constants.COUNT_SMALL);               System.out.println();
        routeAll();
    }

    // ──────────────────────────────────────────────────────────────────
    // Internal helpers
    // ──────────────────────────────────────────────────────────────────

    /** Prints a Unicode box banner to visually separate command output sections. */
    static void banner(String title) {
        String line = "═".repeat(title.length() + 4);
        System.out.println();
        System.out.println("╔" + line + "╗");
        System.out.printf ("║  %s  ║%n", title);
        System.out.println("╚" + line + "╝");
        System.out.println();
    }

    /**
     * Prints a hint showing how to independently verify the source topic
     * offset count via the Kafka CLI, useful after large ingestion runs.
     */
    static void printVerifyHint(long count) {
        System.out.println();
        System.out.println("  Verify offset count in Kafka:");
        System.out.println("    docker exec kafka /opt/kafka/bin/kafka-get-offsets.sh \\");
        System.out.println("      --bootstrap-server localhost:9092 --topic source");
        System.out.printf ("    (expect sum of partition offsets ≈ %,d)%n%n", count);
    }

    static void printUsage() {
        System.out.println("""
            IoT Telemetry Pipeline

            Usage:  java -jar iot-telemetry-pipeline-1.0-SNAPSHOT-jar-with-dependencies.jar <command> [n]

            Commands:
              print              Generate & print 100 sample records to console
              validate           Generate 1,000 records → sample.csv → validate schema
              connect            Send a probe message to verify broker connectivity
              produce <n>        Ingest N records into the source topic  (default: 10,000)
              benchmark          Compare baseline vs tuned producer on 10,000 records
              fullrun            Ingest 50,000,000 records with 5%-interval progress
              route <n>          Produce N records, then consume & route exactly those N records
              routeall           Route all records available in source topic from earliest offset
              countoffsets       Print per-partition offset totals for all pipeline topics
              all                Run every command end-to-end in the correct order

            Examples:
              java -jar app.jar print
              java -jar app.jar produce 1000000
              java -jar app.jar routeall
              java -jar app.jar countoffsets
            """);
    }
}
