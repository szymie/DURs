package org.szymie.client.strong.pessimistic;


import org.springframework.messaging.converter.MappingJackson2MessageConverter;
import org.szymie.client.strong.optimistic.TransactionState;
import org.szymie.client.strong.optimistic.ValueGateway;
import org.szymie.messages.BeginTransactionRequest;
import org.szymie.messages.BeginTransactionResponse;
import org.szymie.messages.CommitRequest;
import org.szymie.messages.CommitResponse;

import java.util.Map;
import java.util.stream.Collectors;

public class WebSocketSerializableTransaction implements Transaction {

    private ValueGateway valueGateway;
    private RemoteGateway remoteGateway;
    private boolean readOnly;

    public WebSocketSerializableTransaction() {
        remoteGateway = new WebSocketRemoteGateway(new MappingJackson2MessageConverter());
        valueGateway = new WebSocketValueGateway(remoteGateway);
    }

    @Override
    public void begin(Map<String, Integer> reads, Map<String, Integer> writes) {

        if(reads.isEmpty() && writes.isEmpty()) {
            return;
        }

        valueGateway.openSession();

        if(writes.isEmpty()) {
            readOnly = true;
        } else {
            BeginTransactionRequest request = new BeginTransactionRequest(reads, writes);

            BeginTransactionResponse response = remoteGateway.sendAndReceive("/replica/begin-transaction", request,
                    "/user/queue/begin-transaction-response", BeginTransactionResponse.class);

            valueGateway.getTransactionData().timestamp = response.getTimestamp();
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

            Map<String, String> writes = valueGateway.getTransactionData().writtenValues.entrySet().stream()
                    .collect(Collectors.toMap(Map.Entry::getKey, entry -> entry.getValue().value));

            writes.forEach((key, value) -> System.err.println(valueGateway.getTransactionData().timestamp + ") " + key + ": " + value));

            CommitRequest request = new CommitRequest(valueGateway.getTransactionData().timestamp, writes);

            CommitResponse response = remoteGateway.sendAndReceive("/replica/commit-transaction", request,
                    "/user/queue/commit-transaction-response", CommitResponse.class);
        }

        valueGateway.closeSession();

        return true;
    }
}
