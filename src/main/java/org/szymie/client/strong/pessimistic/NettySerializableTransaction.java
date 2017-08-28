package org.szymie.client.strong.pessimistic;


import org.szymie.Configuration;
import org.szymie.client.strong.RemoteGateway;
import org.szymie.client.strong.optimistic.*;
import org.szymie.messages.*;

import java.util.Map;
import java.util.stream.Collectors;

public class NettySerializableTransaction implements Transaction {

    private ValueGateway valueGateway;
    private RemoteGateway remoteGateway;
    private boolean readOnly;

    public NettySerializableTransaction() {
        this(new Configuration());
    }

    public NettySerializableTransaction(Configuration configuration) {
        this(0, configuration);
    }

    public NettySerializableTransaction(int numberOfClientThreads, Configuration configuration) {
        remoteGateway = new NettyRemoteGateway(numberOfClientThreads, new ClientChannelInitializer(new PessimisticClientMessageHandlerFactory()));
        valueGateway = new NettyValueGateway(remoteGateway, configuration);
    }

    public long getTimestamp() {
        return valueGateway.getTransactionData().timestamp;
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

            Messages.Message message = Messages.Message.newBuilder()
                    .setBeginTransactionRequest(request)
                    .build();

            Messages.BeginTransactionResponse response = remoteGateway.sendAndReceive(message, Messages.BeginTransactionResponse.class);
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

            Messages.Message message = Messages.Message.newBuilder().setCommitRequest(request).build();

            remoteGateway.sendAndReceive(message, Messages.CommitResponse.class);
        }

        valueGateway.closeSession();

        return true;
    }
}
