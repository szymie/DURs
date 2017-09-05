package org.szymie.client.strong.causal;



import org.szymie.client.strong.optimistic.ValueGateway;
import org.szymie.messages.CausalReadResponse;
import org.szymie.messages.ReadResponse;
import org.szymie.server.strong.causal.ValuesWithTimestamp;
import org.szymie.server.strong.optimistic.ValueWithTimestamp;

import java.util.LinkedList;
import java.util.List;

public abstract class BaseValueGateway {

    protected TransactionData transactionData;
    protected boolean sessionOpen;

    public BaseValueGateway() {
        transactionData = new TransactionData();
        sessionOpen = false;
    }

    public boolean isSessionOpen() {
        return sessionOpen;
    }

    public List<String> read(String key) {

        if(key == null) {
            throw new RuntimeException("empty key cannot be read");
        }

        ValueWithTimestamp<String> value = transactionData.writtenValues.get(key);

        if(value == null) {

            ValuesWithTimestamp<String> values = transactionData.readValues.get(key);

            if(values == null) {
                CausalReadResponse causalReadResponse = readRemotely(key);
                values = new ValuesWithTimestamp<>(causalReadResponse.values, causalReadResponse.timestamp, causalReadResponse.fresh);
            }

            transactionData.readValues.put(key, values);
            return values.values;
        }

        ValuesWithTimestamp<String> values = new ValuesWithTimestamp<>(new LinkedList<String>() {{ add(value.value); }}, value.timestamp, value.fresh);
        transactionData.readValues.put(key, values);
        return values.values;
    }

    protected abstract CausalReadResponse readRemotely(String key);

    public void write(String key, String value) {

        if(key == null) {
            throw new RuntimeException("empty key cannot be written");
        }

        if(value == null) {
            throw new RuntimeException("empty value cannot be written");
        }

        transactionData.writtenValues.put(key, new ValueWithTimestamp<>(value, Long.MAX_VALUE, true));
    }

    public void remove(String key) {

        if(key == null) {
            throw new RuntimeException("empty key cannot be written");
        }

        transactionData.writtenValues.put(key, new ValueWithTimestamp<>(null, Long.MAX_VALUE, true));
    }

    public void clear() {
        transactionData.clear();
    }

    public TransactionData getTransactionData() {
        return transactionData;
    }
}
