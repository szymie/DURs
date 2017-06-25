package org.szymie.client;

import akka.actor.ActorSystem;
import lsr.paxos.client.ReplicationException;
import lsr.paxos.client.SerializableClient;
import org.szymie.messages.CertificationRequest;
import org.szymie.messages.CertificationResponse;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;

public class SerializableTransaction implements Transaction {

    private AkkaValueGateway valueGateway;
    private TransactionState state;
    private SerializableClient client;

    public SerializableTransaction(ActorSystem actorSystem) {

        this.valueGateway = new AkkaValueGateway(actorSystem);
        state = TransactionState.NOT_STARTED;

        try {
            client = new SerializableClient(new lsr.common.Configuration("src/main/resources/paxos.properties"));
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

        TransactionMetadata transactionMetadata = valueGateway.getTransactionMetadata();

        client.connect();

        CertificationRequest request = new CertificationRequest(transactionMetadata.readValues, transactionMetadata.writtenValues, transactionMetadata.timestamp);

        try {

            CertificationResponse response;

            if(request.writtenValues.isEmpty()) {
                response = new CertificationResponse(true);
            } else {
                response = (CertificationResponse) client.execute(request);
            }

            if(response.success) {
                state = TransactionState.COMMITTED;
            } else {
                state = TransactionState.ABORTED;
            }

            valueGateway.closeSession();

            return response.success;
        } catch (IOException | ClassNotFoundException | ReplicationException e) {
            throw new RuntimeException(e);
        }
    }

    public TransactionState getState() {
        return state;
    }
}
