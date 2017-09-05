package org.szymie.server.strong.optimistic;

import com.google.common.collect.Multiset;
import com.google.common.collect.TreeMultiset;
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
import java.util.concurrent.locks.Lock;
import java.util.function.Function;
import java.util.stream.Collectors;

public class SerializableCertificationService extends SerializableService {

    @Value("${id}")
    private int id;

    private ResourceRepository resourceRepository;
    private AtomicLong timestamp;

    private TreeMultiset<Long> liveTransactions;
    private Lock liveTransactionsLock;

    public SerializableCertificationService(ResourceRepository resourceRepository, AtomicLong timestamp,
                                            TreeMultiset<Long> liveTransactions, Lock liveTransactionsLock) {

        this.resourceRepository = resourceRepository;
        this.timestamp = timestamp;
        this.liveTransactions = liveTransactions;
        this.liveTransactionsLock = liveTransactionsLock;
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

        for(Map.Entry<String, ValueWithTimestamp<String>> readValue : request.readValues.entrySet()) {

            Optional<ValueWithTimestamp<String>> valueOptional = resourceRepository.get(readValue.getKey(), Integer.MAX_VALUE);

            if(valueOptional.isPresent()) {
                ValueWithTimestamp<String> value = valueOptional.get();
                if(value.timestamp > request.timestamp) {
                    return false;
                }
            }
        }

        return true;
    }

    private void applyChanges(CertificationRequest request) {

        long time = timestamp.incrementAndGet();
        request.writtenValues.forEach((key, value) -> {

            if(value.isEmpty()) {
                resourceRepository.remove(key, time);
            } else {
                resourceRepository.put(key, value.value, time);
            }
        });

        liveTransactionsLock.lock();

        Multiset.Entry<Long> oldestTransaction = liveTransactions.firstEntry();

        if(oldestTransaction != null) {
            Long oldestTransactionTimestamp = oldestTransaction.getElement();
            resourceRepository.removeOutdatedVersions(oldestTransactionTimestamp);
        }

        liveTransactions.remove(request.timestamp);
        liveTransactionsLock.unlock();
    }

    @Override
    protected void updateToSnapshot(Object o) {
        System.err.println("updateToSnapshot");
        Map.Entry<Long, Map<String, ValueWithTimestamp<String>>> snapshot = (Map.Entry<Long, Map<String, ValueWithTimestamp<String>>>) o;
        snapshot.getValue().forEach((key, valueWithTimestamp) -> resourceRepository.put(key, valueWithTimestamp.value, valueWithTimestamp.timestamp));
        timestamp.set(snapshot.getKey());
    }

    @Override
    protected Object makeObjectSnapshot() {
        System.err.println("makeObjectSnapshot");
        Map<String, ValueWithTimestamp<String>> state = resourceRepository.getKeys().stream()
                .collect(Collectors.toMap(Function.identity(), key -> resourceRepository.get(key, Long.MAX_VALUE).get()));
        return new AbstractMap.SimpleEntry<>(timestamp.longValue(), state);
    }
}
