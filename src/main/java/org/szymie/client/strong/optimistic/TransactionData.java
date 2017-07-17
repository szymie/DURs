package org.szymie.client.strong.optimistic;

import org.szymie.server.strong.optimistic.ValueWithTimestamp;

import java.util.*;

public class TransactionData {

    public Map<String, ValueWithTimestamp> readValues;
    public Map<String, ValueWithTimestamp> writtenValues;
    public long timestamp;

    public TransactionData() {
        readValues = new HashMap<>();
        writtenValues = new HashMap<>();
        clear();
    }

    public void clear() {
        readValues.clear();
        writtenValues.clear();
        timestamp = Long.MAX_VALUE;
    }
}
