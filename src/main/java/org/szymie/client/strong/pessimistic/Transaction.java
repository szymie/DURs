package org.szymie.client.strong.pessimistic;


import java.util.Map;

public interface Transaction {
    void begin(Map<String, Integer> reads, Map<String, Integer> writes);
    String read(String key);
    void write(String key, String value);
    void remove(String key);
    boolean commit();
}
