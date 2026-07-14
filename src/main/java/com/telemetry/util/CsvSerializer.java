package com.telemetry.util;

import com.telemetry.model.Telemetry;

/**
 * Utility to serialize Telemetry objects to CSV strings.
 */
public final class CsvSerializer {

    private CsvSerializer() {}

    /**
     * Serializes a Telemetry record to a CSV line.
     *
     * @param telemetry the telemetry record
     * @return comma-separated values string
     */
    public static String serialize(Telemetry telemetry) {
        if (telemetry == null) {
            return "";
        }
        return telemetry.toCsv();
    }
}
