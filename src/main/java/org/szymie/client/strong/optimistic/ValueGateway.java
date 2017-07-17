package org.szymie.client.strong.optimistic;

public interface ValueGateway {
    void openSession();
    void closeSession();
    boolean isSessionOpen();
    String read(String key);
    void write(String key, String value);
    void remove(String key);
    void clear();
    TransactionData getTransactionData();
}
