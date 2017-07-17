package org.szymie.server.strong.pessimistic;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.messaging.MessageHeaders;
import org.springframework.messaging.handler.annotation.MessageMapping;
import org.springframework.messaging.simp.SimpMessageHeaderAccessor;
import org.springframework.messaging.simp.SimpMessageSendingOperations;
import org.springframework.messaging.simp.SimpMessageType;
import org.springframework.messaging.simp.annotation.SendToUser;
import org.springframework.stereotype.Controller;
import org.szymie.messages.BeginTransactionResponse;

@Controller
public class TransactionController {

    private SimpMessageSendingOperations messagingTemplate;

    @Autowired
    public TransactionController(SimpMessageSendingOperations messagingTemplate) {
        this.messagingTemplate = messagingTemplate;
    }

    @MessageMapping("/begin-transaction")
    @SendToUser("/begin-transaction-responses")
    public void begin(SimpMessageHeaderAccessor headers) throws Exception {

        String sessionId = headers.getSessionId();


        messagingTemplate.convertAndSendToUser(sessionId, "/queue/begin-transaction-responses", new BeginTransactionResponse(), createHeaders(sessionId));


    }

    private MessageHeaders createHeaders(String sessionId) {
        SimpMessageHeaderAccessor headersAccessor = SimpMessageHeaderAccessor.create(SimpMessageType.MESSAGE);
        headersAccessor.setSessionId(sessionId);
        headersAccessor.setLeaveMutable(true);
        return headersAccessor.getMessageHeaders();
    }

}
