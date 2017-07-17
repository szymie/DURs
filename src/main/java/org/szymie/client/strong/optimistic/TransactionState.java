package org.szymie.client.strong.optimistic;

public enum TransactionState {
    NOT_STARTED,
    PROCESSING,
    TERMINATION,
    COMMITTED,
    ABORTED
}
