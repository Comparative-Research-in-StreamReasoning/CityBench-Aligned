package org.insight_centre.aceis.utils.test;

public class ThroughputMeasurer implements Runnable{
    private static boolean stop = false;
    private PerformanceMonitor performanceMonitor;

    public ThroughputMeasurer(PerformanceMonitor performanceMonitor) {
        this.performanceMonitor = performanceMonitor;
    }

    @Override
    public void run() {
        while (!stop) {
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            performanceMonitor.streamedStatementsPerSecond.add(performanceMonitor.streamedStatementInLastSecond);
            performanceMonitor.streamedStatementInLastSecond = 0;
        }
    }

    public static void stop() {
        stop = true;
    }
}
