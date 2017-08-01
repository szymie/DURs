package org.szymie.messages;

import java.io.Serializable;
import java.util.Map;

public class BeginTransactionRequest implements Serializable {

    private Map<String, Integer> reads;
    private Map<String, Integer> writes;

    public BeginTransactionRequest() {
    }

    public BeginTransactionRequest(Map<String, Integer> reads, Map<String, Integer> writes) {
        this.reads = reads;
        this.writes = writes;
    }

    public Map<String, Integer> getReads() {
        return reads;
    }

    public void setReads(Map<String, Integer> reads) {
        this.reads = reads;
    }

    public Map<String, Integer> getWrites() {
        return writes;
    }

    public void setWrites(Map<String, Integer> writes) {
        this.writes = writes;
    }
}
