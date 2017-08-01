package org.szymie.server.strong.pessimistic;

import lsr.paxos.client.ReplicationException;
import lsr.paxos.client.SerializableClient;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Profile;
import org.springframework.messaging.MessageHeaders;
import org.springframework.messaging.handler.annotation.MessageMapping;
import org.springframework.messaging.simp.SimpMessageHeaderAccessor;
import org.springframework.messaging.simp.SimpMessageSendingOperations;
import org.springframework.messaging.simp.SimpMessageType;
import org.springframework.messaging.simp.annotation.SendToUser;
import org.springframework.stereotype.Controller;
import org.szymie.messages.*;
import org.szymie.server.strong.optimistic.ResourceRepository;
import org.szymie.server.strong.optimistic.ValueWithTimestamp;

import java.io.IOException;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicLong;

@Profile("pessimistic")
@Controller
public class TransactionController implements HeadersCreator {

    private BeginTransactionService beginTransactionService;
    private Map<Long, TransactionMetadata> activeTransactions;
    private SimpMessageSendingOperations messagingTemplate;
    private Map<Long, String> sessionIds;
    private GroupMessenger groupMessenger;
    private ResourceRepository resourceRepository;
    private AtomicLong timestamp;
    private SerializableClient client;

    @Autowired
    public TransactionController(BeginTransactionService beginTransactionService, SimpMessageSendingOperations messagingTemplate,
                                 Map<Long, TransactionMetadata> activeTransactions, Map<Long, String> sessionIds,
                                 GroupMessenger groupMessenger, ResourceRepository resourceRepository, AtomicLong timestamp) {
        this.beginTransactionService = beginTransactionService;

        try {
            client = new SerializableClient(new lsr.common.Configuration("src/main/resources/paxos.properties"));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        this.messagingTemplate = messagingTemplate;
        this.activeTransactions = activeTransactions;
        this.sessionIds = sessionIds;
        this.groupMessenger = groupMessenger;
        this.resourceRepository = resourceRepository;
        this.timestamp = timestamp;
    }

    @MessageMapping("/begin-transaction")
    public void begin(BeginTransactionRequest request, SimpMessageHeaderAccessor headers) throws Exception {

        String sessionId = headers.getSessionId();

        client.connect();

        try {

            BeginTransactionResponse response = (BeginTransactionResponse) client.execute(request);

            if(response.isStartPossible()) {
                messagingTemplate.convertAndSendToUser(sessionId, "/queue/begin-transaction-response", response, createHeaders(sessionId));
            }

            sessionIds.put(response.getTimestamp(), sessionId);
        } catch (IOException | ClassNotFoundException | ReplicationException e) {
            e.printStackTrace();
            throw new RuntimeException(e);
        }
    }

    @MessageMapping("/commit-transaction")
    public void commit(CommitRequest request) throws Exception {
        TransactionMetadata transaction = activeTransactions.get(request.getTimestamp());

        long timestamp = request.getTimestamp();
        System.err.println("timestamp: " + timestamp);
        System.err.println("transaction: " + transaction);

        groupMessenger.send(new StateUpdate(request.getTimestamp(), transaction.getApplyAfter(), request.getWrites()));
    }

    @MessageMapping("/read")
    @SendToUser("/queue/read-response")
    public ReadResponse read(ReadRequest request) throws Exception {

        long transactionTimestamp = request.getTimestamp();

        if(transactionTimestamp == Long.MAX_VALUE) {
            transactionTimestamp = timestamp.get();
        }

        Optional<ValueWithTimestamp> valueOptional = resourceRepository.get(request.getKey(), transactionTimestamp);

        return valueOptional.map(valueWithTimestamp ->
                new ReadResponse(valueWithTimestamp.value, valueWithTimestamp.timestamp, valueWithTimestamp.fresh))
                .orElse(new ReadResponse(null, transactionTimestamp, true));
    }
}
