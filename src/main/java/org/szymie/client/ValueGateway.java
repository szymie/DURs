package org.szymie.client;

public interface ValueGateway {
    void openSession();
    void closeSession();
    boolean isSessionOpen();
    String read(String key);
    void write(String key, String value);
    void remove(String key);
    void clear();
    TransactionMetadata getTransactionMetadata();
}
