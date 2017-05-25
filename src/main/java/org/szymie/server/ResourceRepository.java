package org.szymie.server;

import java.util.Map;
import java.util.NavigableMap;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListMap;

public class ResourceRepository {

    private Map<String, NavigableMap<Long, String>> values;

    public ResourceRepository() {
        this.values = new ConcurrentHashMap<>();
    }

    public Optional<Value> get(String key, long timestamp) {

        NavigableMap<Long, String> versions = values.get(key);

        if(versions != null) {

            Map.Entry<Long, String> version = versions.floorEntry(timestamp);

            if(version != null) {
                return Optional.of(new Value(version.getValue(), version.getKey()));
            }
        }

        return Optional.empty();
    }

    public void put(String key, String value, long timestamp) {

        NavigableMap<Long, String> versions = values.get(key);

        if(versions == null) {
            versions = new ConcurrentSkipListMap<>();
        }

        versions.put(timestamp, value);

        values.put(key, versions);
    }

    public Set<String> getKeys() {
        return values.keySet();
    }
}
