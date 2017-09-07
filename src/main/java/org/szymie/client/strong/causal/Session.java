package org.szymie.client.strong.causal;

import org.szymie.Configuration;

public class Session {

    long localClock;
    private boolean open;
    private NettyCausalTransaction currentTransaction;

    public Session() {
        localClock = 0;
        open = false;
        currentTransaction = null;
    }

    public void open() {
        open = true;
        localClock = 0;
    }

    public void close() {
        open = false;
    }

    public NettyCausalTransaction newTransaction() {
        return setCurrentTransaction(new NettyCausalTransaction(this));
    }

    public NettyCausalTransaction newTransaction(Configuration configuration) {
        return setCurrentTransaction(new NettyCausalTransaction(this, configuration));
    }

    public NettyCausalTransaction newTransaction(int numberOfClientThreads, Configuration configuration) {
        return setCurrentTransaction(new NettyCausalTransaction(this, numberOfClientThreads, configuration));
    }

    private NettyCausalTransaction setCurrentTransaction(NettyCausalTransaction transaction) {
        currentTransaction = transaction;
        return currentTransaction;
    }

    public boolean isOpen() {
        return open;
    }
}
