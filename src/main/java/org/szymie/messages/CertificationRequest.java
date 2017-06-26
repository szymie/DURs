package org.szymie.messages;

import org.szymie.ValueWrapper;
import org.szymie.server.ValueWithTimestamp;

import java.io.Serializable;
import java.util.Map;
import java.util.Set;

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
