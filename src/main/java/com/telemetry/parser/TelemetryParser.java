package com.telemetry.parser;

import com.telemetry.model.Telemetry;
import java.util.Optional;

/**
 * Deserializes raw message strings (CSV format) into Telemetry objects.
 */
public final class TelemetryParser {

    private TelemetryParser() {}

    /**
     * Parses a raw CSV line strictly. Throws exceptions on parsing failures.
     *
     * @param rawCsv the raw CSV line from Kafka
     * @return a valid Telemetry object
     * @throws IllegalArgumentException or NumberFormatException if format is invalid
     */
    public static Telemetry parse(String rawCsv) {
        if (rawCsv == null || rawCsv.isBlank()) {
            throw new IllegalArgumentException("CSV content is empty or null");
        }
        return Telemetry.fromCsv(rawCsv);
    }

    /**
     * Parses a raw CSV line safely, returning Optional.empty() on error instead of throwing.
     *
     * @param rawCsv the raw CSV line
     * @return Optional containing the Telemetry object, or Optional.empty() if invalid
     */
    public static Optional<Telemetry> parseSafe(String rawCsv) {
        try {
            return Optional.of(parse(rawCsv));
        } catch (Exception e) {
            return Optional.empty();
        }
    }
}
