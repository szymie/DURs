package org.szymie.server;

import org.szymie.ValueWrapper;

import java.util.Map;
import java.util.NavigableMap;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListMap;

public class ResourceRepository {

    private Map<String, NavigableMap<Long, ValueWrapper<String>>> values;

    public ResourceRepository() {
        this.values = new ConcurrentHashMap<>();
    }

    public Optional<ValueWithTimestamp> get(String key, long timestamp) {

        NavigableMap<Long, ValueWrapper<String>> versions = values.get(key);

        if(versions != null) {

            Map.Entry<Long, ValueWrapper<String>> version = versions.floorEntry(timestamp);
            Map.Entry<Long, ValueWrapper<String>> lastVersion = versions.lastEntry();

            if(lastVersion != null) {
                boolean fresh = version.getKey().equals(lastVersion.getKey());
                return Optional.of(new ValueWithTimestamp(lastVersion.getValue().value, lastVersion.getKey(), fresh));
            }
        }

        return Optional.empty();
    }

    public void put(String key, String value, long timestamp) {

        NavigableMap<Long, ValueWrapper<String>> versions = values.get(key);

        if(versions == null) {
            versions = new ConcurrentSkipListMap<>();
        }

        versions.put(timestamp, new ValueWrapper<>(value));

        values.put(key, versions);
    }

    public void remove(String key, long timestamp) {

        if(get(key, timestamp).isPresent()) {
            put(key, null, timestamp);
        }
    }

    public Set<String> getKeys() {
        return values.keySet();
    }
}
