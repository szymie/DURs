package org.szymie.client.strong.causal;

import org.szymie.server.strong.causal.ValuesWithTimestamp;
import org.szymie.server.strong.optimistic.ValueWithTimestamp;

import java.util.HashMap;
import java.util.Map;

public class TransactionData {

    public Map<String, ValuesWithTimestamp<String>> readValues;
    public Map<String, ValueWithTimestamp<String>> writtenValues;
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

