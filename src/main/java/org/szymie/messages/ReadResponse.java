package org.szymie.messages;

import java.io.Serializable;

public class ReadResponse implements Serializable {

    public final String value;
    public final long timestamp;

    public ReadResponse(String value, long timestamp) {
        this.value = value;
        this.timestamp = timestamp;
    }
}
