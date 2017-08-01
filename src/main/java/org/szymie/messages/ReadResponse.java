package org.szymie.messages;

import java.io.Serializable;

public class ReadResponse implements Serializable {

    public String value;
    public long timestamp;
    public boolean fresh;

    public ReadResponse() {
    }

    public ReadResponse(String value, long timestamp) {
        this.value = value;
        this.timestamp = timestamp;
        fresh = true;
    }

    public ReadResponse(String value, long timestamp, boolean fresh) {
        this.value = value;
        this.timestamp = timestamp;
        this.fresh = fresh;
    }

    public String getValue() {
        return value;
    }

    public void setValue(String value) {
        this.value = value;
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
