package org.szymie.client;

import akka.actor.ActorSystem;
import lsr.paxos.client.Client;

import java.util.Arrays;
import java.util.HashSet;

public class SerializableTransaction implements Transaction {

    private ValueGateway valueGateway;
    private TransactionStatus status;
    private Client client;

    public SerializableTransaction(ActorSystem actorSystem) {
        this.valueGateway = new ValueGateway(actorSystem);
        status = new TransactionStatus();
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
    public boolean commit() {

        checkStatus("", TransactionStates.PROCESSING);

        status.set(TransactionStates.TERMINATION);

        client.connect();
        client.execute();

        if(committed) {
            status.set(TransactionStates.COMMITTED);
        } else {
            status.set(TransactionStates.ABORTED);
        }

        valueGateway.closeSession();

        return committed;
    }

    public TransactionStates getStatus() {
        return status.get();
    }
}
