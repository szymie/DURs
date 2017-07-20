package org.szymie.client.strong.pessimistic;


import org.springframework.messaging.converter.MappingJackson2MessageConverter;
import org.szymie.client.strong.optimistic.TransactionState;
import org.szymie.client.strong.optimistic.ValueGateway;
import org.szymie.messages.BeginTransactionRequest;
import org.szymie.messages.BeginTransactionResponse;

import java.util.Map;

public class SerializableTransaction implements Transaction {

    private ValueGateway valueGateway;
    private RemoteGateway remoteGateway;
    private boolean readOnly;
    private boolean writeOnly;

    public SerializableTransaction() {
        remoteGateway = new WebSocketRemoteGateway(new MappingJackson2MessageConverter());
        valueGateway = new WebSocketValueGateway(remoteGateway);
    }

    @Override
    public void begin(Map<String, Integer> reads, Map<String, Integer> writes) {

        if(reads.isEmpty() && writes.isEmpty()) {
            return;
        }

        if(reads.isEmpty()) {
            writeOnly = true;
        } else {

            valueGateway.openSession();

            if(writes.isEmpty()) {
                readOnly = true;
            } else {
                BeginTransactionRequest request = new BeginTransactionRequest(reads, writes);

                BeginTransactionResponse response = remoteGateway.sendAndReceive("/replica/begin-transaction", request,
                        "/replica/queue/begin-transaction-response", BeginTransactionResponse.class);

                valueGateway.getTransactionData().timestamp = response.getTimestamp();
            }
        }
    }

    @Override
    public String read(String key) {
        return valueGateway.read(key);
    }

    @Override
    public void write(String key, String value) {
        valueGateway.write(key, value);
    }

    @Override
    public void remove(String key) {
        valueGateway.remove(key);
    }

    @Override
    public boolean commit() {

        if(!readOnly) {

            if(writeOnly) {

            } else {

            }
        }

        return true;
    }
}
