package org.szymie.client.strong.optimistic;

import lsr.common.PID;
import lsr.paxos.client.ReplicationException;
import lsr.paxos.client.SerializableClient;
import org.szymie.Configuration;
import org.szymie.PaxosProcessesCreator;
import org.szymie.messages.CertificationRequest;
import org.szymie.messages.CertificationResponse;
import org.szymie.messages.Messages;
import org.szymie.server.strong.JPaxosClientPool;
import org.szymie.server.strong.JPaxosLocalClientPool;

import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;

public class NettySerializableTransaction implements Transaction, PaxosProcessesCreator {

    private NettyRemoteGateway remoteGateway;
    private NettyValueGateway valueGateway;
    private TransactionState state;
    private SerializableClient client;

    private String paxosProcesses;
    private int clientPoolSize;

    public NettySerializableTransaction() {
        this(32);
    }

    public NettySerializableTransaction(int clientPoolSize) {
        this(new Configuration(), clientPoolSize);
    }

    public NettySerializableTransaction(Configuration configuration, int clientPoolSize) {
        this(0, configuration, clientPoolSize);
    }

    public NettySerializableTransaction(int numberOfClientThreads, Configuration configuration, int clientPoolSize) {

        remoteGateway = new NettyRemoteGateway(numberOfClientThreads, new ClientChannelInitializer(new OptimisticClientMessageHandlerFactory()));

        this.valueGateway = new NettyValueGateway(remoteGateway, configuration);
        state = TransactionState.NOT_STARTED;

        this.clientPoolSize = clientPoolSize;
        paxosProcesses = configuration.get("paxosProcesses", "");
        /*List<PID> processes = createPaxosProcesses(paxosProcesses);
        
        try {
            if(processes.isEmpty()) {
                InputStream paxosProperties = getClass().getClassLoader().getResourceAsStream("paxos.properties");
                client = new SerializableClient(new lsr.common.Configuration(paxosProperties));
            } else {
                client = new SerializableClient(new lsr.common.Configuration(processes));
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }*/
    }

    @Override
    public void begin() {
        checkStatus("", TransactionState.NOT_STARTED, TransactionState.COMMITTED, TransactionState.ABORTED);
        state = TransactionState.PROCESSING;
        valueGateway.clear();
    }

    private void checkStatus(String exceptionMessage, TransactionState... statuses) throws WrongTransactionStatus {

        HashSet<TransactionState> statusesSet = new HashSet<>(Arrays.asList(statuses));

        if(!statusesSet.contains(state)) {
            throw new WrongTransactionStatus(exceptionMessage);
        }
    }

    @Override
    public String read(String key) {

        checkStatus("", TransactionState.PROCESSING);

        if(!valueGateway.isSessionOpen()) {
            valueGateway.openSession();
        }

        return valueGateway.read(key);
    }

    @Override
    public void write(String key, String value) {
        checkStatus("", TransactionState.PROCESSING);
        valueGateway.write(key, value);
    }

    @Override
    public void remove(String key) {
        checkStatus("", TransactionState.PROCESSING);
        valueGateway.remove(key);
    }

    @Override
    public boolean commit() {

        checkStatus("", TransactionState.PROCESSING);

        state = TransactionState.TERMINATION;

        TransactionData transactionData = valueGateway.getTransactionData();

        CertificationRequest request = new CertificationRequest(transactionData.readValues, transactionData.writtenValues, transactionData.timestamp);

        CertificationResponse response;

        if(request.writtenValues.isEmpty()) {

            if(!request.readValues.isEmpty()) {

                Messages.CommitRequest commitRequest = Messages.CommitRequest.newBuilder()
                        .setTimestamp(transactionData.timestamp)
                        .build();

                Messages.Message message = Messages.Message.newBuilder()
                        .setCommitRequest(commitRequest)
                        .build();

                remoteGateway.sendAndReceive(message , Messages.CommitResponse.class);
            }

            response = new CertificationResponse(true);
        } else {
            response = commitUpdateTransaction(request);
        }

        if(response.success) {
            state = TransactionState.COMMITTED;
        } else {
            state = TransactionState.ABORTED;
        }

        if(valueGateway.isSessionOpen()) {
            valueGateway.closeSession();
        }

        return response.success;
    }

    private CertificationResponse commitUpdateTransaction(CertificationRequest request) {

        if(checkLocalCondition(request)) {

            client = JPaxosLocalClientPool.get(paxosProcesses, clientPoolSize);

            //client.connect();

            try {
                return (CertificationResponse) client.execute(request);
            } catch (IOException | ClassNotFoundException | ReplicationException e) {
                e.printStackTrace();
                throw new RuntimeException(e);
            }
        }

        return new CertificationResponse(false);
    }

    private boolean checkLocalCondition(CertificationRequest request) {
        return request.readValues.entrySet()
                .stream()
                .allMatch(entry -> entry.getValue().fresh);
    }

    public TransactionState getState() {
        return state;
    }

    public long getTimestamp() {
        return valueGateway.getTransactionData().timestamp;
    }
}
