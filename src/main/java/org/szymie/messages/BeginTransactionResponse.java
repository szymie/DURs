package org.szymie.messages;

import java.io.Serializable;

public class BeginTransactionResponse implements Serializable {

    private long timestamp;

    public BeginTransactionResponse() {
    }

    public BeginTransactionResponse(long timestamp) {
        this.timestamp = timestamp;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }
}
