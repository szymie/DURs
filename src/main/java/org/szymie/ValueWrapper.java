package org.szymie;

import java.io.Serializable;

public class ValueWrapper<T> implements Serializable {

    public T value;

    public ValueWrapper(T value) {
        this.value = value;
    }

    public boolean isEmpty() {
        return value == null;
    }
}
