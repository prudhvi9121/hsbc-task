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
 * IoT Telemetry Pipeline — CLI Orchestrator supporting Milestone 2 and 3 commands.
 */
public class App {

    public static void main(String[] args) throws Exception {
        String command = (args.length > 0) ? args[0].toLowerCase() : "help";
        long   count   = (args.length > 1) ? Long.parseLong(args[1]) : Constants.COUNT_SMALL;

        switch (command) {
            case "print"        -> phasePrint();
            case "validate"     -> phaseValidate();
            case "connect"      -> phaseConnect();
            case "produce"      -> phaseProduce(count);
            case "benchmark"    -> phaseBenchmark();
            case "fullrun"      -> phaseFullRun();
            case "route"        -> phaseRoute(count);
            case "routeall"     -> phaseRouteAll();
            case "countoffsets" -> phaseCountOffsets();
            case "all"          -> runAll();
            default             -> printUsage();
        }
    }

    // ══════════════════════════════════════════════════════════════════
    // Phase 1 — Generate & print 100 records
    // ══════════════════════════════════════════════════════════════════
    static void phasePrint() {
        banner("Phase 1: Generate & Print " + Constants.COUNT_PRINT + " Records");
        TelemetryGenerator gen = new TelemetryGenerator();
        for (int i = 0; i < Constants.COUNT_PRINT; i++) {
            System.out.println(gen.generateTelemetry().toCsv());
        }
        System.out.println();
        System.out.println("─── Done (" + Constants.COUNT_PRINT + " records printed) ───");
    }

    // ══════════════════════════════════════════════════════════════════
    // Phase 2 — Write sample.csv + validate schema
    // ══════════════════════════════════════════════════════════════════
    static void phaseValidate() throws IOException {
        banner("Phase 2: Generate " + Constants.COUNT_VALIDATE + " Records → sample.csv → Validate");
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

    // ══════════════════════════════════════════════════════════════════
    // Phase 3 — Kafka connectivity test
    // ══════════════════════════════════════════════════════════════════
    static void phaseConnect() {
        banner("Phase 3: Kafka Connection Test");
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

    // ══════════════════════════════════════════════════════════════════
    // Phase 4 — Produce N records with the tuned producer
    // ══════════════════════════════════════════════════════════════════
    static void phaseProduce(long count) {
        banner(String.format("Produce %,d Records  (tuned producer)", count));
        TelemetryGenerator gen   = new TelemetryGenerator();
        ProgressTimer      timer = new ProgressTimer();

        try (KafkaProducerService svc =
                 new KafkaProducerService(Constants.TOPIC_SOURCE, ProducerConfig.tuned())) {
            timer.start(count);
            for (long i = 0; i < count; i++) {
                svc.sendAsync(gen.generateTelemetry());
                timer.checkpoint(i + 1);
            }
            svc.flush();    // ensure all buffered records reach Kafka before stopping timer
        }
        timer.stop(count);
        printVerifyHint(count);
    }

    // ══════════════════════════════════════════════════════════════════
    // Phase 5 — Benchmark: baseline vs tuned producer
    // ══════════════════════════════════════════════════════════════════
    static void phaseBenchmark() {
        banner("Benchmark: Baseline vs Tuned Producer (" +
               String.format("%,d", Constants.COUNT_SMALL) + " records each)");

        TelemetryGenerator gen = new TelemetryGenerator();

        // ── Baseline ────────────────────────────────────────────────
        System.out.println("── Baseline (no tuning) ──");
        ProgressTimer baseTimer = new ProgressTimer();
        try (KafkaProducerService svc =
                 new KafkaProducerService(Constants.TOPIC_SOURCE, ProducerConfig.baseline())) {
            baseTimer.start(0);   // timing only, no progress printing
            for (long i = 0; i < Constants.COUNT_SMALL; i++) {
                svc.sendAsync(gen.generateTelemetry());
            }
            svc.flush();
        }
        long baseMs = baseTimer.stop(Constants.COUNT_SMALL);

        System.out.println();

        // ── Tuned ────────────────────────────────────────────────────
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

        // ── Comparison ───────────────────────────────────────────────
        System.out.println();
        double speedup = baseMs > 0 ? (double) baseMs / tunedMs : 1.0;
        System.out.printf("  Baseline : %,.3f sec%n", baseMs  / 1000.0);
        System.out.printf("  Tuned    : %,.3f sec%n", tunedMs / 1000.0);
        System.out.printf("  Speedup  : %.2f×%n",     speedup);
        System.out.println();
    }

    // ══════════════════════════════════════════════════════════════════
    // Phase 6 — Full 50 M-record run
    // ══════════════════════════════════════════════════════════════════
    static void phaseFullRun() {
        banner(String.format("Full Run: %,d Records", Constants.COUNT_FULL));
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

    // ══════════════════════════════════════════════════════════════════
    // Milestone 3 — Consume, Classify, and Route N Records
    // ══════════════════════════════════════════════════════════════════
    static void phaseRoute(long count) {
        banner(String.format("Milestone 3: Route %,d Records (Produce + Consume + Classify + Route)", count));

        // 1. Produce N fresh records to source topic first
        System.out.println("Step 1: Producing " + count + " records to source...");
        phaseProduce(count);
        System.out.println("Flushed producer. Starting consumer and router...");

        // 2. Consume exactly N records and route them
        Properties consumerProps = ConsumerConfig.defaultSettings("route-group-" + System.currentTimeMillis());
        Properties producerProps = ProducerConfig.tuned();
        
        ProgressTimer timer = new ProgressTimer();

        try (KafkaConsumerService consumerSvc = new KafkaConsumerService(Constants.TOPIC_SOURCE, consumerProps);
             KafkaProducerService producerSvc = new KafkaProducerService(Constants.TOPIC_SOURCE, producerProps)) {
            
            MessageRouter router = new MessageRouter(producerSvc);
            
            System.out.println("\nStep 2: Routing records from source to target topics...");
            consumerSvc.consumeExact(count, router, timer);
            
            producerSvc.flush();
            System.out.println("\nRouting complete. Generating verification report:\n");
            router.printReport(count);
        }
    }

    // ══════════════════════════════════════════════════════════════════
    // Milestone 3 — Route All Available Records in source
    // ══════════════════════════════════════════════════════════════════
    static void phaseRouteAll() {
        banner("Milestone 3: Route All Available Source Records");

        Properties consumerProps = ConsumerConfig.defaultSettings("route-all-group");
        Properties producerProps = ProducerConfig.tuned();

        ProgressTimer timer = new ProgressTimer();
        Timer routingTimer = new Timer();

        try (KafkaConsumerService consumerSvc = new KafkaConsumerService(Constants.TOPIC_SOURCE, consumerProps);
             KafkaProducerService producerSvc = new KafkaProducerService(Constants.TOPIC_SOURCE, producerProps)) {
            
            MessageRouter router = new MessageRouter(producerSvc);
            
            routingTimer.start();
            long processed = consumerSvc.consumeAll(router, timer);
            producerSvc.flush();
            routingTimer.stop();

            System.out.println("\nAll source records routed. Verification report:\n");
            router.printReport(processed);
            
            if (processed > 0) {
                routingTimer.printSummary("Routing Throughput", processed);
            }
        }
    }

    // ══════════════════════════════════════════════════════════════════
    // Milestone 3 — Count Offset totals across all topics
    // ══════════════════════════════════════════════════════════════════
    static void phaseCountOffsets() {
        banner("Verify Topic Offsets & Totals");
        
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
                    System.out.printf("Topic: %-20s | (No partitions found or topic doesn't exist)%n", topic);
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

    // ══════════════════════════════════════════════════════════════════
    // Run all phases in order
    // ══════════════════════════════════════════════════════════════════
    static void runAll() throws IOException {
        phasePrint();       System.out.println();
        phaseValidate();    System.out.println();
        phaseConnect();     System.out.println();
        phaseProduce(Constants.COUNT_SMALL);    System.out.println();
        phaseBenchmark();   System.out.println();
        phaseProduce(Constants.COUNT_MEDIUM);   System.out.println();
        phaseFullRun();     System.out.println();
        phaseRoute(Constants.COUNT_SMALL);     System.out.println();
        phaseRouteAll();
    }

    // ══════════════════════════════════════════════════════════════════
    // Helpers
    // ══════════════════════════════════════════════════════════════════

    static void banner(String title) {
        String line = "═".repeat(title.length() + 4);
        System.out.println();
        System.out.println("╔" + line + "╗");
        System.out.printf ("║  %s  ║%n", title);
        System.out.println("╚" + line + "╝");
        System.out.println();
    }

    static void printVerifyHint(long count) {
        System.out.println();
        System.out.println("  Verify offset count in Kafka:");
        System.out.println("    docker exec kafka /opt/kafka/bin/kafka-get-offsets.sh \\");
        System.out.println("      --bootstrap-server localhost:9092 --topic source");
        System.out.printf ("    (expect sum of partition offsets ≈ %,d)%n%n", count);
    }

    static void printUsage() {
        System.out.println("""
            IoT Telemetry Pipeline — Milestone 3

            Usage:  java -jar iot-telemetry-pipeline-1.0-SNAPSHOT-jar-with-dependencies.jar <command> [n]

            Commands:
              print              Generate & print 100 sample records to console
              validate           Generate 1,000 records → sample.csv → validate schema
              connect            Send "Hello Kafka" to verify broker connectivity
              produce <n>        Produce N records to the source topic  (default: 10,000)
              benchmark          Compare baseline vs tuned producer on 10,000 records
              fullrun            Produce 50,000,000 records with 5%-interval progress
              route <n>          Produce N records, then consume & route exactly those N records
              routeall           Route all records currently available in source topic from earliest
              countoffsets       Count offset totals for source, critical, nominal, archive topics
              all                Run every phase in order

            Examples:
              java -jar app.jar print
              java -jar app.jar route 1000
              java -jar app.jar routeall
              java -jar app.jar countoffsets
            """);
    }
}
