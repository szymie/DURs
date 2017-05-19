package org.szymie.client;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

public class SerializableTransaction implements Transaction {

    private ValuesGateway valuesGateway;
    private TransactionStatus status;

    public SerializableTransaction() {
        valuesGateway = new ValuesGateway();
        status = new TransactionStatus();
    }

    @Override
    public void begin() {
        checkStatus("", TransactionStates.NOT_STARTED, TransactionStates.COMMITTED, TransactionStates.ABORTED);
        status.set(TransactionStates.PROCESSING);
        valuesGateway.clear();
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

        if(!valuesGateway.isSessionOpen()) {
            valuesGateway.openSession();
        }

        return valuesGateway.read(key);
    }

    @Override
    public void write(String key, String value) {
        checkStatus("", TransactionStates.PROCESSING);
        valuesGateway.write(key, value);
    }

    @Override
    public boolean commit() {

        checkStatus("", TransactionStates.PROCESSING);

        status.set(TransactionStates.TERMINATION);

        //termination
        boolean committed = true;

        if(committed) {
            status.set(TransactionStates.COMMITTED);
        } else {
            status.set(TransactionStates.ABORTED);
        }

        valuesGateway.closeSession();

        return committed;
    }

    public TransactionStates getStatus() {
        return status.get();
    }
}
