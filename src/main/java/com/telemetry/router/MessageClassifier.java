package com.telemetry.router;

import com.telemetry.model.Telemetry;
import com.telemetry.util.Constants;

/**
 * Message Classifier to determine whether a telemetry record is critical or nominal.
 *
 * Rules:
 * - If the first letter of readingType is A-M (case-insensitive) -> critical
 * - If the first letter of readingType is N-Z (case-insensitive) -> nominal
 */
public final class MessageClassifier {

    private MessageClassifier() {}

    /**
     * Determines if the readingType starts with an A-M character.
     *
     * @param telemetry the telemetry record
     * @return true if critical, false otherwise
     */
    public static boolean isCritical(Telemetry telemetry) {
        if (telemetry == null || telemetry.readingType() == null || telemetry.readingType().isEmpty()) {
            throw new IllegalArgumentException("Telemetry or readingType is null/empty");
        }
        
        char firstChar = Character.toUpperCase(telemetry.readingType().charAt(0));
        return firstChar >= 'A' && firstChar <= 'M';
    }

    /**
     * Determines the destination topic for classification.
     *
     * @param telemetry the telemetry record
     * @return Constants.TOPIC_CRITICAL or Constants.TOPIC_NOMINAL
     */
    public static String classify(Telemetry telemetry) {
        return isCritical(telemetry) ? Constants.TOPIC_CRITICAL : Constants.TOPIC_NOMINAL;
    }

    /**
     * Gets the alphabetical sorted index of a grid region.
     * Africa=0, Asia=1, Australia=2, Europe=3, North America=4, South America=5.
     *
     * @param region the grid region string
     * @return 0-5 index, or -1 if unknown
     */
    public static int getRegionIndex(String region) {
        if (region == null) return -1;
        switch (region) {
            case "Africa": return 0;
            case "Asia": return 1;
            case "Australia": return 2;
            case "Europe": return 3;
            case "North America": return 4;
            case "South America": return 5;
            default: return -1;
        }
    }
}
