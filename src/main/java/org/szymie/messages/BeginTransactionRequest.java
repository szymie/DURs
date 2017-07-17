package org.szymie.messages;

import java.io.Serializable;
import java.util.Map;

public class BeginTransactionRequest implements Serializable {

    public final Map<String, Integer> reads;
    public final Map<String, Integer> writes;

    public BeginTransactionRequest(Map<String, Integer> reads, Map<String, Integer> writes) {
        this.reads = reads;
        this.writes = writes;
    }
}
