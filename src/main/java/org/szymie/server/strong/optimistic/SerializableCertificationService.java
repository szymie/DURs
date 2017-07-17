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
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;
import java.util.stream.Collectors;

@Service
public class SerializableCertificationService extends SerializableService implements DisposableBean {

    @Value("${id}")
    private int id;

    private ResourceRepository resourceRepository;
    private AtomicLong timestamp;

    private AtomicInteger throughputCounter;
    private Thread throughputCounterThread;

    private PerformanceMeasurer performanceMeasurer;
    private List<Double> performanceResults;

    @Autowired
    public SerializableCertificationService(ResourceRepository resourceRepository, AtomicLong timestamp) {

        this.resourceRepository = resourceRepository;
        this.timestamp = timestamp;
        throughputCounter = new AtomicInteger(0);
        performanceResults = new LinkedList<>();

        DecimalFormat decimalFormat = new DecimalFormat("#.##");
        decimalFormat.setRoundingMode(RoundingMode.CEILING);

        performanceMeasurer = new PerformanceMeasurer(10);

        throughputCounterThread = new Thread(() -> {

            while(!Thread.currentThread().isInterrupted()) {

                int currentThroughput = throughputCounter.getAndSet(0);

                performanceMeasurer.addMeasurePoint(currentThroughput);

                double averageThroughput = performanceMeasurer.getThroughput();

                System.err.println(decimalFormat.format(averageThroughput));
                performanceResults.add(averageThroughput);

                try {
                    Thread.sleep(TimeUnit.SECONDS.toMillis(1));
                } catch (InterruptedException ignore) { }
            }
        });

        throughputCounterThread.start();
    }

    @Override
    protected CertificationResponse execute(Object o) {

        CertificationRequest request = (CertificationRequest) o;

        boolean certificationSuccessful = certify(request);

        if(certificationSuccessful) {

            applyChanges(request);

            throughputCounter.incrementAndGet();

            return new CertificationResponse(true);
        } else {
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
    public void destroy() throws Exception {

        throughputCounterThread.interrupt();

        Calendar currentTime = Calendar.getInstance();
        currentTime.setTime(Date.from(Instant.now()));

        String now = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss").format(LocalDateTime.now());

        try(PrintWriter out = new PrintWriter(String.format("results-%s-%d", now, id))) {
            performanceResults.forEach(out::println);
        }
    }
}
