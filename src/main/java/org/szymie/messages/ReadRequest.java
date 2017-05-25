package org.szymie.messages;

import java.io.Serializable;

public class ReadRequest implements Serializable {

    public final String key;
    public final long timestamp;

    public ReadRequest(String key, long timestamp) {
        this.key = key;
        this.timestamp = timestamp;
    }
}
