package org.szymie.server;

import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.stream.Stream;

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
