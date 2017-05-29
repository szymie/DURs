package org.szymie.server;

import lsr.service.SerializableService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.szymie.ValueWrapper;
import org.szymie.messages.CertificationRequest;
import org.szymie.messages.CertificationResponse;

import java.util.AbstractMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;
import java.util.stream.Collectors;

@Service
public class CertificationService extends SerializableService {

    private ResourceRepository resourceRepository;
    private AtomicLong timestamp;

    @Autowired
    public CertificationService(ResourceRepository resourceRepository, AtomicLong timestamp) {
        this.resourceRepository = resourceRepository;
        this.timestamp = timestamp;
    }

    @Override
    protected Object execute(Object o) {

        CertificationRequest request = (CertificationRequest) o;

        System.out.println("timestamp:" + request.timestamp);

        System.out.println("writtenValues");
        request.writtenValues.forEach((s, s2) -> System.out.println(s + " " + s2));

        System.out.println("readValues");

        request.readValues.forEach((s, s2) -> System.out.println(s + " " + s2));

        for(Map.Entry<String, ValueWrapper<String>> readValue : request.readValues.entrySet()) {

            Optional<ValueWithTimestamp> valueOptional = resourceRepository.get(readValue.getKey(), Integer.MAX_VALUE);

            if(valueOptional.isPresent()) {
                ValueWithTimestamp value = valueOptional.get();
                if(value.timestamp > request.timestamp) {
                    return new CertificationResponse(false);
                }
            }
        }

        long time = timestamp.incrementAndGet();
        request.writtenValues.forEach((key, value) -> {

            if(value.isEmpty()) {
                resourceRepository.remove(key, time);
            } else {
                resourceRepository.put(key, value.value, time);
            }
        });

        return new CertificationResponse(true);
    }

    @Override
    protected void updateToSnapshot(Object o) {
        Map.Entry<Long, Map<String, ValueWithTimestamp>> snapshot = (Map.Entry<Long, Map<String, ValueWithTimestamp>>) o;
        snapshot.getValue().forEach((key, valueWithTimestamp) -> resourceRepository.put(key, valueWithTimestamp.value, valueWithTimestamp.timestamp));
        timestamp.set(snapshot.getKey());
    }

    @Override
    protected Object makeObjectSnapshot() {
        Map<String, ValueWithTimestamp> state = resourceRepository.getKeys().stream()
                .collect(Collectors.toMap(Function.identity(), key -> resourceRepository.get(key, Integer.MAX_VALUE).get()));
        return new AbstractMap.SimpleEntry<>(timestamp.longValue(), state);
    }
}
