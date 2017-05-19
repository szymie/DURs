package org.szymie.client;

public class WrongTransactionStatus extends RuntimeException {

    public WrongTransactionStatus() {
    }

    public WrongTransactionStatus(String message) {
        super(message);
    }
}
