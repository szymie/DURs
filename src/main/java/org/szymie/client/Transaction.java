package org.szymie.client;

public interface Transaction {
    void begin();
    String read(String key);
    void write(String key, String value);
    boolean commit();
}
