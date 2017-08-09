package org.szymie.client.strong.optimistic;


import org.szymie.messages.ReadResponse;
import org.szymie.server.strong.optimistic.ValueWithTimestamp;

public abstract class BaseValueGateway implements ValueGateway {

    protected TransactionData transactionData;
    protected boolean sessionOpen;

    public BaseValueGateway() {
        transactionData = new TransactionData();
        sessionOpen = false;
    }

    @Override
    public boolean isSessionOpen() {
        return sessionOpen;
    }

    public String read(String key) {

        if(key == null) {
            throw new RuntimeException("empty key cannot be read");
        }

        ValueWithTimestamp value = transactionData.writtenValues.get(key);

        if(value == null) {

            value = transactionData.readValues.get(key);

            if(value == null) {

                ReadResponse readResponse = readRemotely(key);
                value = new ValueWithTimestamp(readResponse.value, readResponse.timestamp, readResponse.fresh);
            }
        }

        transactionData.readValues.put(key, value);

        return value.value;
    }

    protected abstract ReadResponse readRemotely(String key);

    @Override
    public void write(String key, String value) {

        if(key == null) {
            throw new RuntimeException("empty key cannot be written");
        }

        if(value == null) {
            throw new RuntimeException("empty value cannot be written");
        }

        transactionData.writtenValues.put(key, new ValueWithTimestamp(value, Long.MAX_VALUE, true));
    }

    @Override
    public void remove(String key) {

        if(key == null) {
            throw new RuntimeException("empty key cannot be written");
        }

        transactionData.writtenValues.put(key, new ValueWithTimestamp(null, Long.MAX_VALUE, true));
    }

    @Override
    public void clear() {
        transactionData.clear();
    }

    @Override
    public TransactionData getTransactionData() {
        return transactionData;
    }
}
