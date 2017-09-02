package org.szymie.server.strong.sequential;

import lsr.service.SerializableService;
import org.szymie.messages.Messages;
import org.szymie.server.strong.optimistic.ValueWithTimestamp;

import java.util.AbstractMap;
import java.util.Map;
import java.util.Optional;
import java.util.Random;
import java.util.function.Function;
import java.util.stream.Collectors;

public class SequentialExecutionService extends SerializableService {

    private SimpleResourceRepository simpleResourceRepository;
    private long timestamp;

    public SequentialExecutionService(SimpleResourceRepository simpleResourceRepository) {
        this.simpleResourceRepository = simpleResourceRepository;
        timestamp = 1;
    }

    @Override
    protected Object execute(Object o) {

        Messages.TransactionExecutionRequest request = (Messages.TransactionExecutionRequest) o;

        request.getReadsList().forEach(key -> {
            Optional<String> valueOptional = simpleResourceRepository.get(key);
            String value = valueOptional.orElse("");
            System.err.println("key" + key + ": " + value);
        });

        request.getWritesList().forEach(key -> simpleResourceRepository.put(key, String.valueOf(timestamp)));

        timestamp++;

        return Messages.TransactionExecutionResponse.newBuilder().build();
    }

    @Override
    protected void updateToSnapshot(Object o) {
        System.err.println("updateToSnapshot");
        Map.Entry<Long, Map<String, String>> snapshot = (Map.Entry<Long, Map<String, String>>) o;
        snapshot.getValue().forEach((key, value) -> simpleResourceRepository.put(key, value));
        timestamp = snapshot.getKey();
    }

    @Override
    protected Object makeObjectSnapshot() {
        System.err.println("makeObjectSnapshot");
        Map<String, String> state = simpleResourceRepository.getKeys().stream()
                .collect(Collectors.toMap(Function.identity(), key -> simpleResourceRepository.get(key).get()));
        return new AbstractMap.SimpleEntry<>(timestamp, state);    }
}
