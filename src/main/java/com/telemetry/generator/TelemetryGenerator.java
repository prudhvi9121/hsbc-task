package com.telemetry.generator;

import com.telemetry.model.Telemetry;
import com.telemetry.util.Constants;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.concurrent.ThreadLocalRandom;

/**
 * Generates random, schema-valid Telemetry records.
 *
 * Design choices:
 *   - ThreadLocalRandom is used throughout: it is faster than java.util.Random
 *     and correct for both single-threaded and (future) multi-threaded use.
 *   - Timestamps are distributed uniformly across the calendar year 2026,
 *     giving realistic temporal spread.
 *   - Reading types are random uppercase ASCII strings in [10, 15] chars.
 *   - Regions are drawn uniformly from the six allowed values.
 */
public class TelemetryGenerator {

    // Timestamp bounds: uniform random offset within calendar year 2026
    private static final Instant BASE_TIME   = Instant.parse("2026-01-01T00:00:00Z");
    private static final long    RANGE_SECS  = 365L * 24L * 60L * 60L; // seconds in one year

    // Alphabet for reading type generation (uppercase A-Z only)
    private static final char[] ALPHA = "ABCDEFGHIJKLMNOPQRSTUVWXYZ".toCharArray();

    // ──────────────────────────────────────────────
    // Individual field generators
    // ──────────────────────────────────────────────

    /**
     * Generates a random positive sensor ID.
     * Range: [1, Integer.MAX_VALUE]
     */
    public int generateSensorId() {
        return ThreadLocalRandom.current().nextInt(1, Integer.MAX_VALUE);
    }

    /**
     * Generates a random ISO-8601 UTC timestamp within calendar year 2026.
     * Example: "2026-04-22T13:47:09Z"
     */
    public String generateTimestamp() {
        long offsetSeconds = ThreadLocalRandom.current().nextLong(0, RANGE_SECS);
        return BASE_TIME.plus(offsetSeconds, ChronoUnit.SECONDS).toString();
    }

    /**
     * Generates a random uppercase alphabetic reading-type code.
     * Length is uniformly distributed in [READING_TYPE_MIN_LEN, READING_TYPE_MAX_LEN].
     * Example: "VOLTAGESURGE", "CURRENTFAILX"
     */
    public String generateReadingType() {
        ThreadLocalRandom rng = ThreadLocalRandom.current();
        int length = rng.nextInt(Constants.READING_TYPE_MIN_LEN, Constants.READING_TYPE_MAX_LEN + 1);
        char[] buf = new char[length];
        for (int i = 0; i < length; i++) {
            buf[i] = ALPHA[rng.nextInt(ALPHA.length)];
        }
        return new String(buf);
    }

    /**
     * Picks one of the six allowed grid regions uniformly at random.
     */
    public String generateGridRegion() {
        return Constants.REGIONS[ThreadLocalRandom.current().nextInt(Constants.REGIONS.length)];
    }

    /**
     * Produces a complete, schema-valid Telemetry record by combining all four generators.
     */
    public Telemetry generateTelemetry() {
        return new Telemetry(
            generateSensorId(),
            generateTimestamp(),
            generateReadingType(),
            generateGridRegion()
        );
    }
}
