package org.szymie.server.strong.optimistic;

import java.io.Serializable;

public class ValueWithTimestamp<T> implements Serializable {

    public T value;
    public long timestamp;
    public boolean fresh;

    public ValueWithTimestamp() {
    }

    public ValueWithTimestamp(T value, long timestamp, boolean fresh) {
        this.value = value;
        this.timestamp = timestamp;
        this.fresh = fresh;
    }

    public boolean isEmpty() {
        return value == null;
    }
}
