package org.szymie.server;

public class Value {

    public String value;
    public long timestamp;
    public boolean empty;

    public Value() {
    }

    public Value(String value, long timestamp) {
        this.value = value;
        this.timestamp = timestamp;
    }
}
