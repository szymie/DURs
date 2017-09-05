package org.szymie.messages;


import org.szymie.server.strong.causal.VectorClock;

import java.io.Serializable;
import java.util.Map;

public class CausalCertificationRequest implements Serializable {

    public final int id;
    public final Map<String, String> writtenValues;
    public final long timestamp;
    public final VectorClock vectorClock;

    public CausalCertificationRequest(int id, Map<String, String> writtenValues, long timestamp, VectorClock vectorClock) {
        this.id = id;
        this.writtenValues = writtenValues;
        this.timestamp = timestamp;
        this.vectorClock = vectorClock;
    }
}