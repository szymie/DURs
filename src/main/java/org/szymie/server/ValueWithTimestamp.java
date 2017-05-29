package org.szymie.server;

import java.io.Serializable;

public class ValueWithTimestamp implements Serializable {

    public String value;
    public long timestamp;

    public ValueWithTimestamp() {
    }

    public ValueWithTimestamp(String value, long timestamp) {
        this.value = value;
        this.timestamp = timestamp;
    }
}
