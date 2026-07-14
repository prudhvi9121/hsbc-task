package com.telemetry.util;

import com.telemetry.model.Telemetry;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.time.Instant;
import java.time.format.DateTimeParseException;
import java.util.Set;

/**
 * Validates a generated CSV file against the Telemetry schema rules.
 *
 * Rules enforced per row:
 *   1. Exactly 4 comma-separated columns.
 *   2. No blank / whitespace-only field values.
 *   3. sensorId is a parseable positive integer.
 *   4. timestamp is a valid ISO-8601 UTC instant.
 *   5. readingType length is in [READING_TYPE_MIN_LEN, READING_TYPE_MAX_LEN].
 *   6. gridRegion is one of the six allowed values.
 */
public final class CsvValidator {

    private CsvValidator() {}

    private static final Set<String> VALID_REGIONS = Set.of(Constants.REGIONS);

    /**
     * Validates every non-blank row of the given CSV file.
     * Prints a full pass/fail summary to stdout and lists any failing rows on stderr.
     *
     * @param filePath path to the CSV file to validate
     * @return true if all rows pass, false if any row fails
     */
    public static boolean validate(String filePath) throws IOException {
        System.out.println();
        System.out.println("╔══════════════════════════════════════╗");
        System.out.println("║     CSV Schema Validation Report     ║");
        System.out.println("╠══════════════════════════════════════╣");
        System.out.printf ("║  File: %-30s║%n", filePath);
        System.out.println("╚══════════════════════════════════════╝");

        int total = 0, passed = 0, failed = 0;

        try (BufferedReader reader = new BufferedReader(new FileReader(filePath))) {
            String line;
            while ((line = reader.readLine()) != null) {
                if (line.isBlank()) continue;
                total++;
                String error = validateRow(line);
                if (error == null) {
                    passed++;
                } else {
                    failed++;
                    System.err.printf("  ✗ Row %4d | %s%n  → %s%n", total, error, line);
                }
            }
        }

        System.out.println();
        System.out.printf("  Total rows : %,d%n",  total);
        System.out.printf("  Passed     : %,d%n",  passed);
        System.out.printf("  Failed     : %,d%n",  failed);

        boolean ok = (failed == 0);
        System.out.println();
        if (ok) {
            System.out.println("  ✓ Result : ALL ROWS VALID — schema OK");
        } else {
            System.out.println("  ✗ Result : VALIDATION FAILED — see errors above");
        }
        System.out.println();

        return ok;
    }

    /**
     * Validates a single CSV row.
     *
     * @return null if valid; a human-readable error message if invalid
     */
    public static String validateRow(String line) {
        // Rule 1: exactly 4 columns
        String[] parts = line.split(",", 4);
        if (parts.length != 4) {
            return "Expected 4 columns, got " + parts.length;
        }

        // Rule 2: no blank values
        for (int i = 0; i < 4; i++) {
            if (parts[i].isBlank()) {
                return "Column " + i + " is blank";
            }
        }

        String rawId      = parts[0].trim();
        String rawTs      = parts[1].trim();
        String rawType    = parts[2].trim();
        String rawRegion  = parts[3].trim();

        // Rule 3: valid positive sensorId
        try {
            int id = Integer.parseInt(rawId);
            if (id <= 0) return "sensorId must be positive, got: " + id;
        } catch (NumberFormatException e) {
            return "sensorId is not a valid integer: " + rawId;
        }

        // Rule 4: valid ISO-8601 UTC timestamp
        try {
            Instant.parse(rawTs);
        } catch (DateTimeParseException e) {
            return "Invalid ISO-8601 timestamp: " + rawTs;
        }

        // Rule 5: readingType length in [10, 15]
        int typeLen = rawType.length();
        if (typeLen < Constants.READING_TYPE_MIN_LEN || typeLen > Constants.READING_TYPE_MAX_LEN) {
            return "readingType length " + typeLen + " not in [10,15]: " + rawType;
        }

        // Rule 6: valid grid region
        if (!VALID_REGIONS.contains(rawRegion)) {
            return "Unknown gridRegion: '" + rawRegion + "'";
        }

        return null; // all rules passed
    }
}
