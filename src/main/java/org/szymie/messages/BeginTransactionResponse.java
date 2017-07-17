package org.szymie.messages;

import java.io.Serializable;

public class BeginTransactionResponse implements Serializable {

    private boolean startAllowed;
    private long timestamp;

    public BeginTransactionResponse() {
    }

    public BeginTransactionResponse(boolean startAllowed, long timestamp) {
        this.startAllowed = startAllowed;
        this.timestamp = timestamp;
    }

    public boolean isStartAllowed() {
        return startAllowed;
    }

    public void setStartAllowed(boolean startAllowed) {
        this.startAllowed = startAllowed;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }
}
