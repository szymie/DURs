package org.szymie.client.strong.optimistic;

public class WrongTransactionStatus extends RuntimeException {

    public WrongTransactionStatus() {
    }

    public WrongTransactionStatus(String message) {
        super(message);
    }
}
