package org.szymie;

public class ValueWrapper<T> {

    public T value;

    public ValueWrapper(T value) {
        this.value = value;
    }

    public boolean isEmpty() {
        return value == null;
    }
}
