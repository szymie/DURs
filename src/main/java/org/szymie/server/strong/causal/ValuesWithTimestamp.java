package org.szymie.server.strong.causal;

import java.io.Serializable;
import java.util.List;
import java.util.Set;

public class ValuesWithTimestamp<T> implements Serializable {

    public List<T> values;
    public long timestamp;
    public boolean fresh;

    public ValuesWithTimestamp() {
    }

    public ValuesWithTimestamp(List<T> values, long timestamp, boolean fresh) {
        this.values = values;
        this.timestamp = timestamp;
        this.fresh = fresh;
    }

    public boolean isEmpty() {
        return values == null;
    }
}
