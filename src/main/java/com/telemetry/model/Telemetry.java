package com.telemetry.model;

/**
 * Immutable telemetry record representing a single IoT sensor reading.
 *
 * Schema:
 *   sensorId    – positive signed 32-bit integer
 *   timestamp   – ISO-8601 UTC  (e.g. 2026-07-14T09:15:32Z)
 *   readingType – uppercase alpha code, 10-15 characters
 *   gridRegion  – one of: Asia | Europe | Africa | North America | South America | Australia
 *
 * CSV format (no header):
 *   sensorId,timestamp,readingType,gridRegion
 */
public record Telemetry(int sensorId, String timestamp, String readingType, String gridRegion) {

    /** Renders the record as a comma-separated line (no trailing newline). */
    public String toCsv() {
        return sensorId + "," + timestamp + "," + readingType + "," + gridRegion;
    }

    /**
     * Parses a CSV line back into a Telemetry record.
     * Splits on exactly 4 fields so multi-word regions (e.g. "North America") are handled correctly.
     *
     * @throws IllegalArgumentException if the line does not have exactly 4 comma-separated fields
     */
    public static Telemetry fromCsv(String line) {
        int idx1 = line.indexOf(',');
        if (idx1 == -1) {
            throw new IllegalArgumentException("Expected 4 comma-separated fields, missing first comma in: " + line);
        }
        int idx2 = line.indexOf(',', idx1 + 1);
        if (idx2 == -1) {
            throw new IllegalArgumentException("Expected 4 comma-separated fields, missing second comma in: " + line);
        }
        int idx3 = line.indexOf(',', idx2 + 1);
        if (idx3 == -1) {
            throw new IllegalArgumentException("Expected 4 comma-separated fields, missing third comma in: " + line);
        }
        
        // Optimize: Parse sensor ID directly from CharSequence without allocating substring
        int sensorId = Integer.parseInt(line, 0, idx1, 10);
        String timestamp = line.substring(idx1 + 1, idx2);
        String readingType = line.substring(idx2 + 1, idx3);
        String gridRegion = getConstantRegion(line, idx3 + 1);

        return new Telemetry(
            sensorId,
            timestamp,
            readingType,
            gridRegion
        );
    }

    /**
     * Parses a CSV line safely, returning {@link java.util.Optional#empty()} instead of
     * throwing when the line is malformed. Used by the router to skip corrupt records
     * and keep the pipeline running without interruption.
     */
    public static java.util.Optional<Telemetry> parseSafe(String line) {
        try {
            return java.util.Optional.of(fromCsv(line));
        } catch (Exception e) {
            return java.util.Optional.empty();
        }
    }

    /**
     * Fast region lookup to retrieve pre-allocated String constants, avoiding object allocation.
     */
    private static String getConstantRegion(String line, int start) {
        int s = start;
        int len = line.length();
        while (s < len && line.charAt(s) <= ' ') {
            s++;
        }
        int e = len;
        while (e > s && line.charAt(e - 1) <= ' ') {
            e--;
        }
        int regionLen = e - s;
        if (regionLen == 4 && line.startsWith("Asia", s)) return "Asia";
        if (regionLen == 6) {
            if (line.startsWith("Europe", s)) return "Europe";
            if (line.startsWith("Africa", s)) return "Africa";
        }
        if (regionLen == 9 && line.startsWith("Australia", s)) return "Australia";
        if (regionLen == 13) {
            if (line.startsWith("North America", s)) return "North America";
            if (line.startsWith("South America", s)) return "South America";
        }
        return line.substring(s, e);
    }

    /** Default string representation is the CSV form. */
    @Override
    public String toString() {
        return toCsv();
    }
}
