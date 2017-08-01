package org.szymie.server.strong.pessimistic;


import org.springframework.messaging.MessageHeaders;
import org.springframework.messaging.simp.SimpMessageHeaderAccessor;
import org.springframework.messaging.simp.SimpMessageType;

public interface HeadersCreator {

    default MessageHeaders createHeaders(String sessionId) {
        SimpMessageHeaderAccessor headersAccessor = SimpMessageHeaderAccessor.create(SimpMessageType.MESSAGE);
        headersAccessor.setSessionId(sessionId);
        headersAccessor.setLeaveMutable(true);
        return headersAccessor.getMessageHeaders();
    }
}
