package org.szymie.client;

import org.szymie.ValueWrapper;

import java.util.*;

public class TransactionMetadata {

    public Map<String, ValueWrapper<String>> readValues;
    public Map<String, ValueWrapper<String>> writtenValues;
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
