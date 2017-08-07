package org.szymie.server.strong.optimistic;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Profile;
import org.springframework.messaging.handler.annotation.MessageMapping;
import org.springframework.messaging.simp.SimpMessageHeaderAccessor;
import org.springframework.messaging.simp.SimpMessageSendingOperations;
import org.springframework.messaging.simp.annotation.SubscribeMapping;
import org.springframework.stereotype.Controller;
import org.szymie.messages.ReadRequest;
import org.szymie.messages.ReadResponse;
import org.szymie.server.strong.pessimistic.HeadersCreator;

import java.util.Optional;
import java.util.concurrent.atomic.AtomicLong;

@Profile("optimistic")
@Controller
public class ReadController implements HeadersCreator {

    private SimpMessageSendingOperations messagingTemplate;
    private ResourceRepository resourceRepository;
    private AtomicLong timestamp;

    @Autowired
    public ReadController(SimpMessageSendingOperations messagingTemplate, ResourceRepository resourceRepository, AtomicLong timestamp) {
        this.messagingTemplate = messagingTemplate;
        this.resourceRepository = resourceRepository;
        this.timestamp = timestamp;
    }

    @SubscribeMapping("/queue/read-response")
    public ReadResponse subscribe() {
        return new ReadResponse();
    }

    @MessageMapping("/read")
    //@SendToUser("/queue/read-response")
    public void read(ReadRequest request, SimpMessageHeaderAccessor headers) throws Exception {

        String sessionId = headers.getSessionId();

        long transactionTimestamp = request.getTimestamp() == Long.MAX_VALUE ? timestamp.get() : request.getTimestamp();

        Optional<ValueWithTimestamp> valueOptional = resourceRepository.get(request.getKey(), transactionTimestamp);

        ReadResponse response = valueOptional.map(valueWithTimestamp ->
                new ReadResponse(valueWithTimestamp.value, transactionTimestamp, valueWithTimestamp.fresh))
                .orElse(new ReadResponse(null, transactionTimestamp, true));

        messagingTemplate.convertAndSendToUser(sessionId, "/queue/read-response", response, createHeaders(sessionId));
    }
}
