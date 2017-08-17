package org.szymie.server.strong.optimistic;

import lsr.service.SerializableService;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import org.szymie.messages.CertificationRequest;
import org.szymie.messages.CertificationResponse;

import java.io.PrintWriter;
import java.math.RoundingMode;
import java.text.DecimalFormat;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;
import java.util.stream.Collectors;

public class SerializableCertificationService extends SerializableService {

    @Value("${id}")
    private int id;

    private ResourceRepository resourceRepository;
    private AtomicLong timestamp;

    private ConcurrentSkipListSet<Long> liveTransactions;

    public SerializableCertificationService(ResourceRepository resourceRepository, AtomicLong timestamp, ConcurrentSkipListSet<Long> liveTransactions) {

        this.resourceRepository = resourceRepository;
        this.timestamp = timestamp;
        this.liveTransactions = liveTransactions;
    }

    @Override
    protected CertificationResponse execute(Object o) {

        CertificationRequest request = (CertificationRequest) o;

        System.err.println("REQUEST");

        boolean certificationSuccessful = certify(request);

        if(certificationSuccessful) {

            applyChanges(request);
            System.err.println("TRUE");

            return new CertificationResponse(true);
        } else {
            System.err.println("FALSE");
            return new CertificationResponse(false);
        }
    }

    private boolean certify(CertificationRequest request) {

        for(Map.Entry<String, ValueWithTimestamp> readValue : request.readValues.entrySet()) {

            Optional<ValueWithTimestamp> valueOptional = resourceRepository.get(readValue.getKey(), Integer.MAX_VALUE);

            if(valueOptional.isPresent()) {
                ValueWithTimestamp value = valueOptional.get();
                if(value.timestamp > request.timestamp) {
                    return false;
                }
            }
        }

        return true;
    }

    private void applyChanges(CertificationRequest request) {

        Long oldestTransaction = liveTransactions.pollFirst();

        if(oldestTransaction != null) {
            resourceRepository.removeOutdatedVersions(oldestTransaction);
        }

        long time = timestamp.incrementAndGet();
        request.writtenValues.forEach((key, value) -> {

            if(value.isEmpty()) {
                resourceRepository.remove(key, time);
            } else {
                resourceRepository.put(key, value.value, time);
            }
        });
    }

    @Override
    protected void updateToSnapshot(Object o) {
        System.err.println("updateToSnapshot");
        Map.Entry<Long, Map<String, ValueWithTimestamp>> snapshot = (Map.Entry<Long, Map<String, ValueWithTimestamp>>) o;
        snapshot.getValue().forEach((key, valueWithTimestamp) -> resourceRepository.put(key, valueWithTimestamp.value, valueWithTimestamp.timestamp));
        timestamp.set(snapshot.getKey());
    }

    @Override
    protected Object makeObjectSnapshot() {
        System.err.println("makeObjectSnapshot");
        Map<String, ValueWithTimestamp> state = resourceRepository.getKeys().stream()
                .collect(Collectors.toMap(Function.identity(), key -> resourceRepository.get(key, Long.MAX_VALUE).get()));
        return new AbstractMap.SimpleEntry<>(timestamp.longValue(), state);
    }
}
