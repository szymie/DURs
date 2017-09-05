package org.szymie.server.strong.causal;

import org.szymie.ValueWrapper;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListMap;

public class CausalResourceRepository {

    private Map<String, NavigableMap<Long, ValueWrapper<List<String>>>> values;

    public CausalResourceRepository() {
        this.values = new ConcurrentHashMap<>();
    }

    public Optional<ValuesWithTimestamp<String>> get(String key, long startTimestamp) {

        NavigableMap<Long, ValueWrapper<List<String>>> versions = values.get(key);

        if(versions != null) {

            Map.Entry<Long, ValueWrapper<List<String>>> version = versions.floorEntry(startTimestamp);
            Map.Entry<Long, ValueWrapper<List<String>>> lastVersion = versions.lastEntry();

            if(version != null && lastVersion != null) {
                boolean fresh = version.getKey().equals(lastVersion.getKey());
                return Optional.of(new ValuesWithTimestamp<>(lastVersion.getValue().value, lastVersion.getKey(), fresh));
            }
        }

        return Optional.empty();
    }

    public void put(String key, String value, long startTimestamp, long endTimestamp) {

        NavigableMap<Long, ValueWrapper<List<String>>> versions = values.get(key);

        if(versions == null) {
            versions = new ConcurrentSkipListMap<>();
            versions.put(endTimestamp, new ValueWrapper<>(Collections.singletonList(value)));
            values.put(key, versions);
        } else {

            long lastKey = versions.lastKey();

            if(lastKey > startTimestamp) {

                ValueWrapper<List<String>> lastValue = versions.lastEntry().getValue();

                List<String> newValues = new LinkedList<>(lastValue.value);
                newValues.add(value);

                versions.put(endTimestamp, new ValueWrapper<>(newValues));
                values.put(key, versions);
            } else {
                versions.put(endTimestamp, new ValueWrapper<>(Collections.singletonList(value)));
                values.put(key, versions);
            }
        }
    }

    void put(String key, List<String> valueList, long timestamp) {

        NavigableMap<Long, ValueWrapper<List<String>>> versions = values.get(key);

        if(versions == null) {
            versions = new ConcurrentSkipListMap<>();
        }

        versions.put(timestamp, new ValueWrapper<>(new LinkedList<>(valueList)));
        values.put(key, versions);
    }

    public void remove(String key, long startTimestamp, long endTimestamp) {

        if(get(key, endTimestamp).isPresent()) {
            put(key, null, startTimestamp, endTimestamp);
        }
    }

    public Set<String> getKeys() {
        return values.keySet();
    }

    public void removeOutdatedVersions(long version) {
        values.forEach((key, versions) -> {
            Long highestKey = versions.lastKey();
            Long upperBound = Math.min(highestKey, version);
            versions.headMap(upperBound).clear();
        });
    }

    public void clear() {
        values.clear();
    }
}
