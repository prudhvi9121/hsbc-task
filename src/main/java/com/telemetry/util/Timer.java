package com.telemetry.util;

/**
 * Timing utility to measure elapsed time and record performance (records/sec, messages/sec).
 */
public class Timer {

    private long startTime;
    private long endTime;

    /** Starts the timer. */
    public void start() {
        this.startTime = System.nanoTime();
    }

    /** Stops the timer. */
    public void stop() {
        this.endTime = System.nanoTime();
    }

    /**
     * Gets the total elapsed time in milliseconds.
     *
     * @return elapsed time in ms
     */
    public long getElapsedMs() {
        return (endTime - startTime) / 1_000_000L;
    }

    /**
     * Gets the total elapsed time in seconds.
     *
     * @return elapsed time in seconds
     */
    public double getElapsedSeconds() {
        return getElapsedMs() / 1000.0;
    }

    /**
     * Calculates throughput (records per second).
     *
     * @param recordCount total records processed
     * @return records/sec
     */
    public long getThroughput(long recordCount) {
        long ms = getElapsedMs();
        if (ms <= 0) return 0;
        return (recordCount * 1000L) / ms;
    }

    /** Prints a clean timing summary. */
    public void printSummary(String label, long recordCount) {
        double secs = getElapsedSeconds();
        long tput = getThroughput(recordCount);
        System.out.printf("[%s] Processed %,d records in %,.3f seconds (%,d rec/s)%n",
            label, recordCount, secs, tput);
    }
}
