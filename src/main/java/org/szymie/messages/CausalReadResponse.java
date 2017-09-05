package org.szymie.messages;

import java.io.Serializable;
import java.util.List;

public class CausalReadResponse implements Serializable {

    public List<String> values;
    public long timestamp;
    public boolean fresh;

    public CausalReadResponse() {
    }

    public CausalReadResponse(List<String> value, long timestamp) {
        this.values = value;
        this.timestamp = timestamp;
        fresh = true;
    }

    public CausalReadResponse(List<String> value, long timestamp, boolean fresh) {
        this.values = value;
        this.timestamp = timestamp;
        this.fresh = fresh;
    }

    public List<String> getValues() {
        return values;
    }

    public void setValues(List<String> values) {
        this.values = values;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }

    public boolean isFresh() {
        return fresh;
    }

    public void setFresh(boolean fresh) {
        this.fresh = fresh;
    }
}
