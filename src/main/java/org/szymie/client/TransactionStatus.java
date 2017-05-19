package org.szymie.client;

public class TransactionStatus {

    private TransactionStates status;

    public TransactionStatus() {
        set(TransactionStates.NOT_STARTED);
    }

    public void set(TransactionStates status) {
        this.status = status;
    }

    public TransactionStates get() {
        return status;
    }

    public boolean is(TransactionStates status) {
        return this.status == status;
    }
}
