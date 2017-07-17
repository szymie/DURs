package org.szymie.server.strong.optimistic;

import java.util.Arrays;

public class PerformanceMeasurer {

    private int measurePointWidth;
    private int[] measurementPoints;
    private int currentIndex;

    public PerformanceMeasurer(int measurePointWidth) {
        this.measurePointWidth = measurePointWidth;
        measurementPoints = new int[measurePointWidth];
        currentIndex = 0;
    }

    public void addMeasurePoint(int throughput) {
        measurementPoints[currentIndex] = throughput;
        currentIndex = (currentIndex + 1) % measurePointWidth;
    }

    public double getThroughput() {
        return Arrays.stream(measurementPoints)
            .mapToDouble(value -> value)
            .average().orElse(0);
    }
}
