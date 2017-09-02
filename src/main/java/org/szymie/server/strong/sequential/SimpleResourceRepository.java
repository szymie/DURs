package org.szymie.server.strong.sequential;


import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

public class SimpleResourceRepository {

    private Map<String, String> values;

    public SimpleResourceRepository() {
        this.values = new HashMap<>();
    }

    public Optional<String> get(String key) {
        return Optional.ofNullable(values.get(key));
    }

    public void put(String key, String value) {
        values.put(key, value);
    }

    public void remove(String key) {
        values.remove(key);
    }

    public Set<String> getKeys() {
        return values.keySet();
    }

    public void clear() {
        values.clear();
    }
}
