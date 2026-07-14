package com.telemetry.util;

/**
 * Central constants for the IoT Telemetry Pipeline.
 * All magic numbers and configuration literals live here.
 */
public final class Constants {

    private Constants() {}

    // ──────────────────────────────────────────────
    // Kafka
    // ──────────────────────────────────────────────
    public static final String BOOTSTRAP_SERVERS = System.getenv().getOrDefault("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092");
    public static final String TOPIC_SOURCE       = "source";
    public static final String TOPIC_CRITICAL      = "critical";
    public static final String TOPIC_NOMINAL       = "nominal";
    public static final String TOPIC_ARCHIVE       = "regional_archive";

    // ──────────────────────────────────────────────
    // Telemetry schema
    // ──────────────────────────────────────────────
    /** The six allowed grid regions. */
    public static final String[] REGIONS = {
        "Asia", "Europe", "Africa", "North America", "South America", "Australia"
    };

    /** The six allowed grid regions sorted alphabetically. */
    public static final String[] SORTED_REGIONS = {
        "Africa", "Asia", "Australia", "Europe", "North America", "South America"
    };

    public static final int READING_TYPE_MIN_LEN = 10;
    public static final int READING_TYPE_MAX_LEN = 15;

    // ──────────────────────────────────────────────
    // Phased record counts
    // ──────────────────────────────────────────────
    public static final int  COUNT_PRINT    =         100;
    public static final int  COUNT_VALIDATE =       1_000;
    public static final int  COUNT_SMALL    =      10_000;
    public static final long COUNT_MEDIUM   =   1_000_000L;
    public static final long COUNT_FULL     =  50_000_000L;
}
