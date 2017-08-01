package org.szymie.messages;

import java.io.Serializable;

public class BeginTransactionResponse implements Serializable {

    private long timestamp;
    private boolean startPossible;

    public BeginTransactionResponse() {
    }

    public BeginTransactionResponse(long timestamp, boolean startPossible) {
        this.timestamp = timestamp;
        this.startPossible = startPossible;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }

    public boolean isStartPossible() {
        return startPossible;
    }

    public void setStartPossible(boolean startPossible) {
        this.startPossible = startPossible;
    }
}
