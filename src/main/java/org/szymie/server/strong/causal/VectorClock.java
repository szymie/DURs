package org.szymie.server.strong.causal;

import akka.util.Collections$;

import java.io.Serializable;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class VectorClock implements Serializable {

    private int id;
    private long[] vector;

    public VectorClock(int id, int size) {
        this.id = id;
        vector = new long[size];
    }

    private VectorClock(int id, long[] vector) {
        this.id = id;
        this.vector = vector;
    }

    public synchronized VectorClock getAndIncrement() {
        long[] vectorAcc = Arrays.copyOf(vector, vector.length);
        vector[id]++;
        return new VectorClock(id, vectorAcc);
    }

    public synchronized void increment(int index) {
        vector[index]++;
    }

    public boolean caused(VectorClock receivedVectorClock) {

        boolean caused = true;

        for(int i = 0; i < vector.length; i++) {
            if(vector[i] < receivedVectorClock.vector[i]) {
                caused = false;
                break;
            }
        }

        return caused;
    }

    @Override
    public String toString() {
        List<String> vectorPositions = Arrays.stream(vector).mapToObj(String::valueOf).collect(Collectors.toList());
        return "[" + String.join(", ", vectorPositions) + "]";
    }
}
