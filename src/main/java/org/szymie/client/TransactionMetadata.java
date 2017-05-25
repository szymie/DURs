package org.szymie.client;

import java.util.HashMap;
import java.util.Map;

public class TransactionMetadata {

    public Map<String, String> readValues;
    public Map<String, String> writtenValues;
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
