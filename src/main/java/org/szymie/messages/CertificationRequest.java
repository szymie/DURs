package org.szymie.messages;

import org.szymie.server.strong.optimistic.ValueWithTimestamp;

import java.io.Serializable;
import java.util.Map;

public class CertificationRequest implements Serializable {

    public final Map<String, ValueWithTimestamp> readValues;
    public final Map<String, ValueWithTimestamp> writtenValues;
    public final long timestamp;

    public CertificationRequest(Map<String, ValueWithTimestamp> readValues, Map<String, ValueWithTimestamp> writtenValues, long timestamp) {
        this.readValues = readValues;
        this.writtenValues = writtenValues;
        this.timestamp = timestamp;
    }
}
