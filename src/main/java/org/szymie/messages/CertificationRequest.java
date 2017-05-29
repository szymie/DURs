package org.szymie.messages;

import org.szymie.ValueWrapper;

import java.io.Serializable;
import java.util.Map;
import java.util.Set;

public class CertificationRequest implements Serializable {

    public final Map<String, ValueWrapper<String>> readValues;
    public final Map<String, ValueWrapper<String>> writtenValues;
    public final long timestamp;

    public CertificationRequest(Map<String, ValueWrapper<String>> readValues, Map<String, ValueWrapper<String>> writtenValues, long timestamp) {
        this.readValues = readValues;
        this.writtenValues = writtenValues;
        this.timestamp = timestamp;
    }
}
