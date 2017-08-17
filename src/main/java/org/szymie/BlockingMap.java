package org.szymie;

import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;

public class BlockingMap<K, V> {

    private final Map<K, BlockingQueue<V>> map = new ConcurrentHashMap<>();

    private synchronized BlockingQueue<V> ensureQueueExists(K key) {

        if (map.containsKey(key)) {
            return map.get(key);
        } else {
            BlockingQueue<V> queue = new ArrayBlockingQueue<>(1);
            map.put(key, queue);
            return queue;
        }
    }

    public boolean put(K key, V value) {

        BlockingQueue<V> queue = ensureQueueExists(key);

        try {
            queue.put(value);
            return queue.offer(value);
        } catch (InterruptedException e) {
            throw new IllegalStateException(e);
        }
    }

    public V get(K key) {

        BlockingQueue<V> queue = ensureQueueExists(key);

        try {
            V value = queue.take();
            queue.put(value);
            return value;
        } catch (InterruptedException e) {
            throw new IllegalStateException(e);
        }
    }

    public V getOrNull(K key) {

        BlockingQueue<V> value = map.get(key);

        if(value != null) {
            return value.peek();
        }

        return null;
    }

    public void remove(K key) {
        map.remove(key);
    }
}