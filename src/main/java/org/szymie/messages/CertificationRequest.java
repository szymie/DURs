package org.szymie.messages;

import java.io.Serializable;
import java.util.Map;
import java.util.Set;

public class CertificationRequest implements Serializable {

    public final Map<String, String> readValues;
    public final Map<String, String> writtenValues;
    public final long timestamp;

    public CertificationRequest(Map<String, String> readValues, Map<String, String> writtenValues, long timestamp) {
        this.readValues = readValues;
        this.writtenValues = writtenValues;
        this.timestamp = timestamp;
    }
}
