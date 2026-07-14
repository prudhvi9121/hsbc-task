package com.telemetry;

import com.telemetry.generator.TelemetryGenerator;
import com.telemetry.model.Telemetry;
import com.telemetry.parser.TelemetryParser;
import com.telemetry.router.MessageClassifier;
import com.telemetry.util.Constants;
import com.telemetry.util.CsvValidator;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Path;
import java.time.Instant;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for Milestones 2 and 3 components.
 */
class AppTest {

    private final TelemetryGenerator gen = new TelemetryGenerator();

    // ──────────────────────────────────────────────
    // TelemetryGenerator
    // ──────────────────────────────────────────────

    @Test
    void sensorId_isPositive() {
        for (int i = 0; i < 1000; i++) {
            int id = gen.generateSensorId();
            assertTrue(id > 0, "sensorId must be positive, got: " + id);
        }
    }

    @Test
    void timestamp_isValidIso8601() {
        for (int i = 0; i < 100; i++) {
            String ts = gen.generateTimestamp();
            assertDoesNotThrow(() -> Instant.parse(ts),
                "Timestamp must parse as ISO-8601: " + ts);
        }
    }

    @Test
    void readingType_lengthInRange() {
        for (int i = 0; i < 1000; i++) {
            String rt = gen.generateReadingType();
            int len = rt.length();
            assertTrue(len >= Constants.READING_TYPE_MIN_LEN && len <= Constants.READING_TYPE_MAX_LEN,
                "readingType length " + len + " not in [10,15]: " + rt);
        }
    }

    @Test
    void gridRegion_isAllowedValue() {
        var allowed = java.util.Set.of(Constants.REGIONS);
        for (int i = 0; i < 1000; i++) {
            String region = gen.generateGridRegion();
            assertTrue(allowed.contains(region), "Unexpected region: " + region);
        }
    }

    @Test
    void generateTelemetry_allFieldsPopulated() {
        Telemetry t = gen.generateTelemetry();
        assertNotNull(t);
        assertTrue(t.sensorId() > 0);
        assertNotNull(t.timestamp());
        assertNotNull(t.readingType());
        assertNotNull(t.gridRegion());
    }

    // ──────────────────────────────────────────────
    // Telemetry CSV round-trip
    // ──────────────────────────────────────────────

    @Test
    void toCsv_hasFourFields() {
        Telemetry t = gen.generateTelemetry();
        String[] parts = t.toCsv().split(",", 4);
        assertEquals(4, parts.length, "CSV must have exactly 4 fields: " + t.toCsv());
    }

    @Test
    void csvRoundTrip_exact() {
        Telemetry original = gen.generateTelemetry();
        String csv         = original.toCsv();
        Telemetry restored = Telemetry.fromCsv(csv);
        assertEquals(original, restored, "Round-trip must reproduce identical record");
    }

    @Test
    void csvRoundTrip_regionWithSpace() {
        Telemetry t = new Telemetry(42, "2026-03-01T00:00:00Z", "ABCDEFGHIJ", "North America");
        assertEquals(t, Telemetry.fromCsv(t.toCsv()));
    }

    // ──────────────────────────────────────────────
    // TelemetryParser
    // ──────────────────────────────────────────────

    @Test
    void parser_strictParse_valid() {
        Telemetry t = TelemetryParser.parse("42,2026-03-01T00:00:00Z,ABCDEFGHIJ,Asia");
        assertEquals(42, t.sensorId());
        assertEquals("Asia", t.gridRegion());
    }

    @Test
    void parser_strictParse_invalidThrows() {
        assertThrows(IllegalArgumentException.class, () -> TelemetryParser.parse(""));
        assertThrows(IllegalArgumentException.class, () -> TelemetryParser.parse("1,2,3"));
    }

    @Test
    void parser_safeParse_valid() {
        Optional<Telemetry> opt = TelemetryParser.parseSafe("42,2026-03-01T00:00:00Z,ABCDEFGHIJ,Asia");
        assertTrue(opt.isPresent());
        assertEquals(42, opt.get().sensorId());
    }

    @Test
    void parser_safeParse_invalidReturnsEmpty() {
        Optional<Telemetry> opt = TelemetryParser.parseSafe("1,2,3");
        assertFalse(opt.isPresent());
    }

    // ──────────────────────────────────────────────
    // MessageClassifier
    // ──────────────────────────────────────────────

    @Test
    void classifier_critical_A_to_M() {
        // AMP_OVERFLOW (A) -> critical
        Telemetry t1 = new Telemetry(1, "2026-03-01T00:00:00Z", "AMP_OVERFLOW", "Asia");
        assertTrue(MessageClassifier.isCritical(t1));
        assertEquals(Constants.TOPIC_CRITICAL, MessageClassifier.classify(t1));

        // BATTERY_LOW (B) -> critical
        Telemetry t2 = new Telemetry(1, "2026-03-01T00:00:00Z", "BATTERY_LOW", "Asia");
        assertTrue(MessageClassifier.isCritical(t2));
        assertEquals(Constants.TOPIC_CRITICAL, MessageClassifier.classify(t2));

        // Last critical char boundary check: M
        Telemetry t3 = new Telemetry(1, "2026-03-01T00:00:00Z", "MAX_LOAD_REACHED", "Asia");
        assertTrue(MessageClassifier.isCritical(t3));
        assertEquals(Constants.TOPIC_CRITICAL, MessageClassifier.classify(t3));
    }

    @Test
    void classifier_nominal_N_to_Z() {
        // THERMAL_CRIT (T) -> nominal
        Telemetry t1 = new Telemetry(1, "2026-03-01T00:00:00Z", "THERMAL_CRIT", "Asia");
        assertFalse(MessageClassifier.isCritical(t1));
        assertEquals(Constants.TOPIC_NOMINAL, MessageClassifier.classify(t1));

        // ZERO_CURRENT (Z) -> nominal
        Telemetry t2 = new Telemetry(1, "2026-03-01T00:00:00Z", "ZERO_CURRENT", "Asia");
        assertFalse(MessageClassifier.isCritical(t2));
        assertEquals(Constants.TOPIC_NOMINAL, MessageClassifier.classify(t2));

        // First nominal char boundary check: N
        Telemetry t3 = new Telemetry(1, "2026-03-01T00:00:00Z", "NOMINAL_VOLT", "Asia");
        assertFalse(MessageClassifier.isCritical(t3));
        assertEquals(Constants.TOPIC_NOMINAL, MessageClassifier.classify(t3));
    }

    @Test
    void classifier_caseInsensitivity() {
        Telemetry lowerCrit = new Telemetry(1, "2026-03-01T00:00:00Z", "amp_overflow", "Asia");
        assertTrue(MessageClassifier.isCritical(lowerCrit));

        Telemetry lowerNom = new Telemetry(1, "2026-03-01T00:00:00Z", "thermal_crit", "Asia");
        assertFalse(MessageClassifier.isCritical(lowerNom));
    }

    // ──────────────────────────────────────────────
    // CsvValidator
    // ──────────────────────────────────────────────

    @Test
    void csvValidator_acceptsValidRow() {
        assertNull(CsvValidator.validateRow(
            "12345,2026-07-14T09:15:32Z,VOLTAGESURGE,Asia"));
    }

    @Test
    void csvValidator_rejectsBlankField() {
        assertNotNull(CsvValidator.validateRow("12345,,VOLTAGESURGE,Asia"));
    }

    @Test
    void csvValidator_rejectsBadTimestamp() {
        assertNotNull(CsvValidator.validateRow("12345,not-a-date,VOLTAGESURGE,Asia"));
    }

    @Test
    void csvValidator_rejectsShortReadingType() {
        assertNotNull(CsvValidator.validateRow("12345,2026-07-14T09:15:32Z,SHORT,Asia"));
    }

    @Test
    void csvValidator_rejectsUnknownRegion() {
        assertNotNull(CsvValidator.validateRow("12345,2026-07-14T09:15:32Z,VOLTAGESURGE,Mars"));
    }

    @Test
    void csvValidator_acceptsNorthAmerica() {
        assertNull(CsvValidator.validateRow(
            "9999,2026-07-14T09:15:32Z,CURRENTFAILX,North America"));
    }

    @Test
    void csvValidator_fullFileValidation(@TempDir Path tmpDir) throws IOException {
        Path csvFile = tmpDir.resolve("test.csv");
        try (BufferedWriter bw = new BufferedWriter(new FileWriter(csvFile.toFile()))) {
            for (int i = 0; i < 500; i++) {
                bw.write(gen.generateTelemetry().toCsv());
                bw.newLine();
            }
        }
        assertTrue(CsvValidator.validate(csvFile.toString()), "500 generated rows must all pass");
    }

    // ──────────────────────────────────────────────
    // Constants
    // ──────────────────────────────────────────────

    @Test
    void constants_topicNamesCorrect() {
        assertEquals("source",           Constants.TOPIC_SOURCE);
        assertEquals("critical",         Constants.TOPIC_CRITICAL);
        assertEquals("nominal",          Constants.TOPIC_NOMINAL);
        assertEquals("regional_archive", Constants.TOPIC_ARCHIVE);
    }

    @Test
    void constants_sixRegions() {
        assertEquals(6, Constants.REGIONS.length);
    }

    @Test
    void classifier_regionIndexAlphabetical() {
        assertEquals(0, MessageClassifier.getRegionIndex("Africa"));
        assertEquals(1, MessageClassifier.getRegionIndex("Asia"));
        assertEquals(2, MessageClassifier.getRegionIndex("Australia"));
        assertEquals(3, MessageClassifier.getRegionIndex("Europe"));
        assertEquals(4, MessageClassifier.getRegionIndex("North America"));
        assertEquals(5, MessageClassifier.getRegionIndex("South America"));
        assertEquals(-1, MessageClassifier.getRegionIndex("Unknown"));
    }

    @Test
    void parser_regionStringDeduped() {
        Telemetry t1 = Telemetry.fromCsv("1,2026-03-01T00:00:00Z,ABCDEFGHIJ,Asia");
        Telemetry t2 = Telemetry.fromCsv("2,2026-03-01T00:00:00Z,ABCDEFGHIJ,Asia");
        // Verify referential equality to ensure we aren't allocating new strings for regions
        assertSame(t1.gridRegion(), t2.gridRegion());
        assertSame("Asia", t1.gridRegion());
        
        Telemetry t3 = Telemetry.fromCsv("3,2026-03-01T00:00:00Z,ABCDEFGHIJ,North America");
        assertSame("North America", t3.gridRegion());
    }
}
