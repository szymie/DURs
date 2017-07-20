package org.szymie.messages;

import java.util.Map;

public class StateUpdate {

    private long timestamp;
    private Map<String, String> writes;

    public StateUpdate() {
    }

    public StateUpdate(long timestamp, Map<String, String> writes) {
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
