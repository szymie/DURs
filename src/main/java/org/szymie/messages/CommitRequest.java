package org.szymie.messages;

import java.io.Serializable;
import java.util.Map;

public class CommitRequest implements Serializable {

    private long timestamp;
    private Map<String, String> writes;

    public CommitRequest() {
    }

    public CommitRequest(long timestamp, Map<String, String> writes) {
        this.timestamp = timestamp;
        this.writes = writes;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }

    public Map<String, String> getWrites() {
        return writes;
    }

    public void setWrites(Map<String, String> writes) {
        this.writes = writes;
    }
}
