package org.szymie.server.strong.causal;

import java.util.Arrays;

public class VectorClock {

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

    public synchronized VectorClock getIncremented() {
        long[] vectorAcc = Arrays.copyOf(vector, vector.length);
        vectorAcc[id]++;
        return new VectorClock(id, vectorAcc);
    }

    public synchronized void increment() {
        vector[id]++;
    }

    public boolean caused(VectorClock receivedVectorClock) {

        boolean caused = true;
        int receivedId = receivedVectorClock.id;

        if(vector[receivedId] + 1 == (receivedVectorClock.vector[receivedId])) {

            for(int i = 0; i < vector.length; i++) {
                if((i != receivedId) && (vector[i] < receivedVectorClock.vector[i])) {
                    caused = false;
                    break;
                }
            }
        } else {
            caused = false;
        }

        return caused;
    }
}
