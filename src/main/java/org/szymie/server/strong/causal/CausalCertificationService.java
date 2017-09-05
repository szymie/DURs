package org.szymie.server.strong.causal;


import com.google.common.collect.Multiset;
import com.google.common.collect.TreeMultiset;
import lsr.service.SerializableService;
import org.springframework.beans.factory.annotation.Value;
import org.szymie.messages.CausalCertificationRequest;
import org.szymie.messages.CertificationResponse;
import java.util.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Lock;
import java.util.function.Function;
import java.util.stream.Collectors;

public class CausalCertificationService extends SerializableService {

    @Value("${id}")
    private int id;

    private CausalResourceRepository resourceRepository;
    private AtomicLong timestamp;

    private TreeMultiset<Long> liveTransactions;
    private Lock liveTransactionsLock;

    private VectorClock vectorClock;

    private LinkedList<CausalCertificationRequest> waitingRequests;

    public CausalCertificationService(CausalResourceRepository resourceRepository, AtomicLong timestamp,
                                      TreeMultiset<Long> liveTransactions, Lock liveTransactionsLock,
                                      VectorClock vectorClock) {

        this.resourceRepository = resourceRepository;
        this.timestamp = timestamp;
        this.liveTransactions = liveTransactions;
        this.liveTransactionsLock = liveTransactionsLock;
        this.vectorClock = vectorClock;
        waitingRequests = new LinkedList<>();
    }

    @Override
    protected CertificationResponse execute(Object o) {
        CausalCertificationRequest request = (CausalCertificationRequest) o;

        vectorClock.caused(request.vectorClock)



        applyChanges(request);
        return new CertificationResponse(true);
    }


    private void applyChanges(CausalCertificationRequest request) {

        long time = timestamp.incrementAndGet();
        request.writtenValues.forEach((key, value) -> {

            if(value.isEmpty()) {
                resourceRepository.remove(key, request.timestamp, time);
            } else {
                resourceRepository.put(key, value, request.timestamp, time);
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
        Map.Entry<Long, Map<String, ValuesWithTimestamp<String>>> snapshot = (Map.Entry<Long, Map<String, ValuesWithTimestamp<String>>>) o;
        snapshot.getValue().forEach((key, valuesWithTimestamp) -> resourceRepository.put(key, valuesWithTimestamp.values, valuesWithTimestamp.timestamp));
        timestamp.set(snapshot.getKey());
    }

    @Override
    protected Object makeObjectSnapshot() {
        System.err.println("makeObjectSnapshot");
        Map<String, ValuesWithTimestamp<String>> state = resourceRepository.getKeys().stream()
                .collect(Collectors.toMap(Function.identity(), key -> resourceRepository.get(key, Long.MAX_VALUE).get()));
        return new AbstractMap.SimpleEntry<>(timestamp.longValue(), state);
    }
}
