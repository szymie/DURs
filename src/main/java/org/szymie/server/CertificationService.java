package org.szymie.server;

import lsr.service.SerializableService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.szymie.messages.CertificationRequest;
import org.szymie.messages.CertificationResponse;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;

@Service
public class CertificationService extends SerializableService {

    private final ResourceRepository resourceRepository;
    private long timestamp;

    @Autowired
    public CertificationService(ResourceRepository resourceRepository) {
        this.resourceRepository = resourceRepository;
        timestamp = 0;
    }

    @Override
    protected Object execute(Object o) {

        CertificationRequest request = (CertificationRequest) o;

        for(String key : request.readValues) {
            Optional<Value> valueOptional = resourceRepository.get(key, Integer.MAX_VALUE);
            if(valueOptional.isPresent()) {
                Value value = valueOptional.get();
                if(value.timestamp > request.timestamp) {
                    return new CertificationResponse(false);
                }
            }
        }

        timestamp++;
        request.writtenValues.forEach((key, value) -> resourceRepository.put(key, value, timestamp));

        return new CertificationResponse(true);
    }

    @Override
    protected void updateToSnapshot(Object o) {
        Map<String, Value> snapshot = (Map<String, Value>) o;
        snapshot.forEach((key, value) -> resourceRepository.put(key, value.value, value.timestamp));
    }

    @Override
    protected Object makeObjectSnapshot() {
         return resourceRepository.getKeys().stream()
                 .collect(Collectors.toMap(Function.identity(), key -> resourceRepository.get(key, Integer.MAX_VALUE)));
    }
}
