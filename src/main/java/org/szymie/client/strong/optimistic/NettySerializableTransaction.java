package org.szymie.client.strong.optimistic;

import lsr.paxos.client.ReplicationException;
import lsr.paxos.client.SerializableClient;
import org.szymie.messages.CertificationRequest;
import org.szymie.messages.CertificationResponse;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;

public class NettySerializableTransaction implements Transaction {

    private NettyValueGateway valueGateway;
    private TransactionState state;
    private SerializableClient client;

    public NettySerializableTransaction() {

        this.valueGateway = new NettyValueGateway(new NettyRemoteGateway());
        state = TransactionState.NOT_STARTED;

        try {
            client = new SerializableClient(new lsr.common.Configuration("paxos.properties"));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
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
            response = new CertificationResponse(true);
        } else {
            response = commitUpdateTransaction(request);
        }

        if(response.success) {
            state = TransactionState.COMMITTED;
        } else {
            state = TransactionState.ABORTED;
        }

        valueGateway.closeSession();

        return response.success;
    }

    private CertificationResponse commitUpdateTransaction(CertificationRequest request) {

        if(checkLocalCondition(request)) {

            client.connect();

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
}
