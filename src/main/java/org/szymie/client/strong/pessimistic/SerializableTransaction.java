package org.szymie.client.strong.pessimistic;


import org.szymie.messages.BeginTransactionRequest;

import java.util.Map;

public class SerializableTransaction implements Transaction {

    RemoteGateway remoteGateway;


    @Override
    public void begin(Map<String, Integer> reads, Map<String, Integer> writes) {

        BeginTransactionRequest request = new BeginTransactionRequest(reads, writes);

        remoteGateway.send("");

    }

    @Override
    public String read(String key) {
        return null;
    }

    @Override
    public void write(String key, String value) {

    }

    @Override
    public void remove(String key) {

    }

    @Override
    public boolean commit() {
        return false;
    }
}
