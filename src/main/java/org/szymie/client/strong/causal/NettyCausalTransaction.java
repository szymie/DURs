package org.szymie.client.strong.causal;

import lsr.common.PID;
import lsr.paxos.client.ReplicationException;
import lsr.paxos.client.SerializableClient;
import org.szymie.Configuration;
import org.szymie.PaxosProcessesCreator;
import org.szymie.client.strong.optimistic.*;
import org.szymie.messages.CertificationRequest;
import org.szymie.messages.CertificationResponse;
import org.szymie.messages.Messages;

import java.io.IOException;
import java.io.InputStream;
import java.util.*;
import java.util.stream.Collectors;

public class NettyCausalTransaction implements PaxosProcessesCreator {

    private NettyRemoteGateway remoteGateway;
    private NettyCausalValueGateway valueGateway;
    private TransactionState state;
    private SerializableClient client;

    public NettyCausalTransaction() {
        this(new Configuration());
    }

    public NettyCausalTransaction(Configuration configuration) {
        this(0, configuration);
    }

    public NettyCausalTransaction(int numberOfClientThreads, Configuration configuration) {

        remoteGateway = new NettyRemoteGateway(numberOfClientThreads, new ClientChannelInitializer(new CausalClientMessageHandlerFactory()));

        this.valueGateway = new NettyCausalValueGateway(remoteGateway, configuration);
    }

    public void begin() {
        valueGateway.clear();
    }

    public List<String> read(String key) {

        if(!valueGateway.isSessionOpen()) {
            valueGateway.openSession();
        }

        return valueGateway.read(key);
    }

    public void write(String key, String value) {
        valueGateway.write(key, value);
    }

    public void remove(String key) {
        valueGateway.remove(key);
    }

    public boolean commit() {

        TransactionData transactionData = valueGateway.getTransactionData();

        Map<String, String> writes = transactionData.writtenValues.entrySet().stream()
                .collect(Collectors.toMap(Map.Entry::getKey, entry -> entry.getValue().value));

        Messages.CommitRequest request = Messages.CommitRequest.newBuilder()
                .setTimestamp(transactionData.timestamp)
                .putAllWrites(writes)
                .build();

        if(writes.isEmpty()) {

            if(!transactionData.readValues.isEmpty()) {

                Messages.Message message = Messages.Message.newBuilder()
                        .setCommitRequest(Messages.CommitRequest.newBuilder().setTimestamp(transactionData.timestamp))
                        .build();

                remoteGateway.sendAndReceive(message , Messages.CommitResponse.class);
            }
        } else {
             commitUpdateTransaction(request);
        }

        if(valueGateway.isSessionOpen()) {
            valueGateway.closeSession();
        }

        return true;
    }

    private void commitUpdateTransaction(Messages.CommitRequest request) {

        Messages.Message message = Messages.Message.newBuilder()
                .setCommitRequest(request)
                .build();

        remoteGateway.sendAndReceive(message , Messages.CommitResponse.class);
    }


    public TransactionState getState() {
        return state;
    }

    public long getTimestamp() {
        return valueGateway.getTransactionData().timestamp;
    }
}
