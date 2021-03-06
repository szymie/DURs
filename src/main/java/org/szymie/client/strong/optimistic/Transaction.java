package org.szymie.client.strong.optimistic;

public interface Transaction {
    void begin();
    String read(String key);
    void write(String key, String value);
    void remove(String key);
    boolean commit();
}
