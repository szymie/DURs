package org.szymie.server;

import lsr.service.SerializableService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.szymie.ValueWrapper;
import org.szymie.messages.CertificationRequest;
import org.szymie.messages.CertificationResponse;

import java.io.IOException;
import java.math.RoundingMode;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.text.DecimalFormat;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;
import java.util.stream.Collectors;

@Service
public class CertificationService extends SerializableService {

    private ResourceRepository resourceRepository;
    private AtomicLong timestamp;

    private AtomicInteger throughputCounter;
    private Thread throughputCounterThread;

    private PerformanceMeasurer performanceMeasurer;

    @Autowired
    public CertificationService(ResourceRepository resourceRepository, AtomicLong timestamp) {

        this.resourceRepository = resourceRepository;
        this.timestamp = timestamp;
        throughputCounter = new AtomicInteger(0);

        DecimalFormat decimalFormat = new DecimalFormat("#.##");
        decimalFormat.setRoundingMode(RoundingMode.CEILING);

        performanceMeasurer = new PerformanceMeasurer(10);

        throughputCounterThread = new Thread(() -> {

            while(!Thread.currentThread().isInterrupted()) {

                int throughput = throughputCounter.getAndSet(0);

                performanceMeasurer.addMeasurePoint(throughput);

                System.err.println(decimalFormat.format(performanceMeasurer.getThroughput()));

                try {
                    Thread.sleep(1000);
                } catch (InterruptedException ignore) { }
            }
        });

        throughputCounterThread.start();
    }

    @Override
    protected Object execute(Object o) {

        CertificationRequest request = (CertificationRequest) o;

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

        throughputCounter.incrementAndGet();

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



    @Override
    protected void finalize() throws Throwable {
        super.finalize();
        throughputCounterThread.interrupt();
    }
}
