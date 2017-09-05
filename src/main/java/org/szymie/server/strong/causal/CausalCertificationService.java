package org.szymie.server.strong.causal;


import com.google.common.collect.Multiset;
import com.google.common.collect.TreeMultiset;
import lsr.service.SerializableService;
import org.springframework.beans.factory.annotation.Value;
import org.szymie.BlockingMap;
import org.szymie.messages.CausalCertificationRequest;
import org.szymie.messages.CausalCertificationResponse;
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

    private LinkedList<Request> requests;
    private long sequentialNumber;
    private BlockingMap<Long, Boolean> responses;

    private class Request {

        int id;
        long sequentialNumber;
        CausalCertificationRequest certificationRequest;

        public Request(int id, long sequentialNumber, CausalCertificationRequest certificationRequest) {
            this.id = id;
            this.sequentialNumber = sequentialNumber;
            this.certificationRequest = certificationRequest;
        }
    }

    public CausalCertificationService(CausalResourceRepository resourceRepository, AtomicLong timestamp,
                                      TreeMultiset<Long> liveTransactions, Lock liveTransactionsLock,
                                      VectorClock vectorClock,
                                      BlockingMap<Long, Boolean> responses) {

        this.resourceRepository = resourceRepository;
        this.timestamp = timestamp;
        this.liveTransactions = liveTransactions;
        this.liveTransactionsLock = liveTransactionsLock;
        this.vectorClock = vectorClock;
        requests = new LinkedList<>();
        sequentialNumber = 0;
        this.responses = responses;
    }

    @Override
    protected CausalCertificationResponse execute(Object o) {

        CausalCertificationRequest certificationRequest = (CausalCertificationRequest) o;

        if(vectorClock.caused(certificationRequest.vectorClock)) {

            deliver(new Request(certificationRequest.id, sequentialNumber, certificationRequest));

            boolean delivered;

            do {

                delivered = false;

                for(Iterator<Request> it = requests.iterator(); it.hasNext();) {

                    Request request = it.next();

                    if(vectorClock.caused(request.certificationRequest.vectorClock)) {
                        deliver(request);
                        it.remove();
                        delivered = true;
                    }
                }

            } while(delivered);


        } else {
            requests.add(new Request(id, sequentialNumber, certificationRequest));
        }

        return new CausalCertificationResponse(true, sequentialNumber++);
    }

    private void deliver(Request request) {

        System.err.println("delivering " + request.certificationRequest.vectorClock);

        applyChanges(request.certificationRequest);

        if(request.id != id) {
            vectorClock.increment(request.id);

            System.err.println("incremented" + vectorClock);

        } else {
            responses.put(request.sequentialNumber, true);
        }

        System.err.println("after delivery " + vectorClock);
        System.err.println("at id: " + id);
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
