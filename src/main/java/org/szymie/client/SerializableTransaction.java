package org.szymie.client;

import akka.actor.ActorSystem;
import lsr.paxos.client.Client;
import lsr.paxos.client.ReplicationException;
import lsr.paxos.client.SerializableClient;
import org.szymie.messages.CertificationRequest;
import org.szymie.messages.CertificationResponse;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;

public class SerializableTransaction implements Transaction {

    private ValueGateway valueGateway;
    private TransactionStatus status;
    private SerializableClient client;

    public SerializableTransaction(ActorSystem actorSystem) {

        this.valueGateway = new ValueGateway(actorSystem);
        status = new TransactionStatus();

        try {
            client = new SerializableClient(new lsr.common.Configuration("src/main/resources/paxos.properties"));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void begin() {
        checkStatus("", TransactionStates.NOT_STARTED, TransactionStates.COMMITTED, TransactionStates.ABORTED);
        status.set(TransactionStates.PROCESSING);
        valueGateway.clear();
    }

    public void checkStatus(String exceptionMessage, TransactionStates... statuses) throws WrongTransactionStatus {

        HashSet<TransactionStates> statusesSet = new HashSet<>(Arrays.asList(statuses));

        if(!statusesSet.contains(status.get())) {
            throw new WrongTransactionStatus(exceptionMessage);
        }
    }

    @Override
    public String read(String key) {

        checkStatus("", TransactionStates.PROCESSING);

        if(!valueGateway.isSessionOpen()) {
            valueGateway.openSession();
        }

        return valueGateway.read(key);
    }

    @Override
    public void write(String key, String value) {
        checkStatus("", TransactionStates.PROCESSING);
        valueGateway.write(key, value);
    }

    @Override
    public void remove(String key) {
        checkStatus("", TransactionStates.PROCESSING);
        valueGateway.remove(key);
    }

    @Override
    public boolean commit() {

        checkStatus("", TransactionStates.PROCESSING);

        status.set(TransactionStates.TERMINATION);

        TransactionMetadata transactionMetadata = valueGateway.getTransactionMetadata();

        client.connect();

        CertificationRequest request = new CertificationRequest(transactionMetadata.readValues, transactionMetadata.writtenValues, transactionMetadata.timestamp);

        try {

            CertificationResponse response = (CertificationResponse) client.execute(request);

            if(response.success) {
                status.set(TransactionStates.COMMITTED);
            } else {
                status.set(TransactionStates.ABORTED);
            }

            valueGateway.closeSession();

            return response.success;
        } catch (IOException | ClassNotFoundException | ReplicationException e) {
            throw new RuntimeException(e);
        }
    }

    public TransactionStates getStatus() {
        return status.get();
    }
}
