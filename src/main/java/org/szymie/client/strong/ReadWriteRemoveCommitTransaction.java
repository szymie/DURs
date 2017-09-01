package org.szymie.client.strong;

public interface ReadWriteRemoveCommitTransaction {
    String read(String key);
    void write(String key, String value);
    void remove(String key);
    boolean commit();
    long getTimestamp();
}
