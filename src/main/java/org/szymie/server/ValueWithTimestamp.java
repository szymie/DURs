package org.szymie.server;

import java.io.Serializable;

public class ValueWithTimestamp implements Serializable {

    public String value;
    public long timestamp;
    public boolean fresh;

    public ValueWithTimestamp() {
    }

    public ValueWithTimestamp(String value) {
        this.value = value;
        timestamp = Long.MIN_VALUE;
        fresh = true;
    }

    public ValueWithTimestamp(String value, long timestamp, boolean fresh) {
        this.value = value;
        this.timestamp = timestamp;
        this.fresh = fresh;
    }

    public boolean isEmpty() {
        return value == null;
    }
}
