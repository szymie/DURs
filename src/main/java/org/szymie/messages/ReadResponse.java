package org.szymie.messages;

public class ReadResponse {

    public final String value;
    public final long timestamp;
    public final boolean empty;

    public ReadResponse(String value, long timestamp, boolean empty) {
        this.value = value;
        this.timestamp = timestamp;
        this.empty = empty;
    }
}
