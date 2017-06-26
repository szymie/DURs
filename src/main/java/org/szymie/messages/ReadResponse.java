package org.szymie.messages;

import java.io.Serializable;

public class ReadResponse implements Serializable {

    public final String value;
    public final long timestamp;
    public final boolean fresh;

    public ReadResponse(String value, long timestamp, boolean fresh) {
        this.value = value;
        this.timestamp = timestamp;
        this.fresh = fresh;
    }
}
