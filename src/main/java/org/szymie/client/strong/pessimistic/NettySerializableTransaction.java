package org.szymie.client.strong.pessimistic;


import org.springframework.messaging.converter.MappingJackson2MessageConverter;
import org.szymie.client.strong.optimistic.ClientChannelInitializer;
import org.szymie.client.strong.optimistic.NettyRemoteGateway;
import org.szymie.client.strong.optimistic.TransactionState;
import org.szymie.client.strong.optimistic.ValueGateway;
import org.szymie.messages.*;

import java.util.Map;
import java.util.stream.Collectors;

public class NettySerializableTransaction implements Transaction {

    private ValueGateway valueGateway;
    private RemoteGateway remoteGateway;
    private boolean readOnly;

    public NettySerializableTransaction() {
        remoteGateway = new NettyRemoteGateway(new ClientChannelInitializer(new PessimisticClientMessageHandlerFactory()));
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

            Messages.BeginTransactionRequest request = Messages.BeginTransactionRequest.newBuilder()
                    .putAllReads(reads)
                    .putAllWrites(writes)
                    .build();

            Messages.BeginTransactionResponse response = remoteGateway.sendAndReceive(request, Messages.BeginTransactionResponse.class);
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

            Messages.CommitRequest request = Messages.CommitRequest.newBuilder()
                    .setTimestamp(valueGateway.getTransactionData().timestamp)
                    .putAllWrites(writes)
                    .build();

            remoteGateway.sendAndReceive(request, Messages.CommitResponse.class);
        }

        valueGateway.closeSession();

        return true;
    }
}
