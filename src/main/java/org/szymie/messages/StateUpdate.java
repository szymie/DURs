package org.szymie.messages;

import java.io.Serializable;
import java.util.Map;

public class StateUpdate implements Serializable, Comparable<StateUpdate> {

    private long timestamp;
    private long applyAfter;
    private Map<String, String> writes;

    public StateUpdate() {
    }

    public StateUpdate(long timestamp, long applyAfter, Map<String, String> writes) {
        this.timestamp = timestamp;
        this.applyAfter = applyAfter;
        this.writes = writes;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }

    public long getApplyAfter() {
        return applyAfter;
    }

    public void setApplyAfter(long applyAfter) {
        this.applyAfter = applyAfter;
    }

    public Map<String, String> getWrites() {
        return writes;
    }

    public void setWrites(Map<String, String> writes) {
        this.writes = writes;
    }

    @Override
    public int compareTo(StateUpdate o) {

        if(timestamp > o.getTimestamp()) {
            return 1;
        } else if(timestamp < o.getTimestamp()) {
            return -1;
        } else {
            return 0;
        }
    }
}
