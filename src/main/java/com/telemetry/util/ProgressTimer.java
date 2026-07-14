package com.telemetry.util;

/**
 * Wall-clock timer with optional 5%-interval progress reporting.
 *
 * Usage (with progress):
 *   ProgressTimer t = new ProgressTimer();
 *   t.start(totalRecords);
 *   for (long i = 0; i < total; i++) {
 *       // ... do work ...
 *       t.checkpoint(i + 1);
 *   }
 *   long elapsedMs = t.stop(totalRecords);
 *
 * Usage (timing only, no progress):
 *   t.start(0);
 *   // ... do work ...
 *   long elapsedMs = t.stop(count);
 */
public class ProgressTimer {

    private long startNanos;
    private long total;
    private long milestoneStep;
    private long nextMilestone;

    /**
     * Starts the timer.
     *
     * @param total number of records expected; set to 0 to disable progress printing
     */
    public void start(long total) {
        this.total          = total;
        this.startNanos     = System.nanoTime();
        this.milestoneStep  = total > 0 ? Math.max(1, total / 20) : Long.MAX_VALUE;
        this.nextMilestone  = milestoneStep;

        if (total > 0) {
            System.out.printf("▶  Starting: %,d records%n", total);
        }
    }

    /**
     * Prints a progress line whenever a 5%-boundary is crossed.
     * No-op if total was 0 at start() time.
     *
     * @param sent number of records sent so far (1-indexed)
     */
    public void checkpoint(long sent) {
        if (total > 0 && sent >= nextMilestone) {
            long ms  = elapsedMs();
            double pct = 100.0 * sent / total;
            long rps = ms > 0 ? (sent * 1000L / ms) : 0L;
            System.out.printf("   %5.1f%%  |  %,14d records  |  %6.1f s  |  %,12d rec/s%n",
                pct, sent, ms / 1000.0, rps);
            nextMilestone += milestoneStep;
        }
    }

    /**
     * Stops the timer, prints a formatted performance summary, and returns elapsed milliseconds.
     *
     * @param recordCount actual number of records produced (may differ from `total` at start())
     * @return elapsed wall-clock milliseconds
     */
    public long stop(long recordCount) {
        long ms  = elapsedMs();
        long rps = ms > 0 ? (recordCount * 1000L / ms) : 0L;

        System.out.println();
        System.out.println("  ┌────────────────────────────────────────┐");
        System.out.printf ("  │  Records produced : %,18d  │%n", recordCount);
        System.out.printf ("  │  Total time       : %,15.3f sec  │%n", ms / 1000.0);
        System.out.printf ("  │  Throughput       : %,16d /s  │%n", rps);
        System.out.println("  └────────────────────────────────────────┘");

        return ms;
    }

    /** Returns milliseconds elapsed since start(). */
    public long elapsedMs() {
        return (System.nanoTime() - startNanos) / 1_000_000L;
    }
}
