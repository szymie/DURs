package org.szymie.messages;

import java.io.Serializable;

public class ReadRequest implements Serializable {

    private String key;
    private long timestamp;

    public ReadRequest() {
    }

    public ReadRequest(String key, long timestamp) {
        this.key = key;
        this.timestamp = timestamp;
    }

    public String getKey() {
        return key;
    }

    public void setKey(String key) {
        this.key = key;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }
}
