package org.szymie.client;

import org.szymie.ValueWrapper;
import org.szymie.server.ValueWithTimestamp;

import java.util.*;

public class TransactionMetadata {

    public Map<String, ValueWithTimestamp> readValues;
    public Map<String, ValueWithTimestamp> writtenValues;
    public long timestamp;

    public TransactionMetadata() {
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
