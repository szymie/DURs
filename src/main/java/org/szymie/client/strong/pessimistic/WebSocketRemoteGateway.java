package org.szymie.client.strong.pessimistic;

import org.springframework.messaging.converter.MappingJackson2MessageConverter;
import org.springframework.messaging.converter.MessageConverter;
import org.springframework.messaging.simp.stomp.*;
import org.springframework.util.concurrent.ListenableFuture;
import org.springframework.web.socket.client.WebSocketClient;
import org.springframework.web.socket.client.standard.StandardWebSocketClient;
import org.springframework.web.socket.messaging.WebSocketStompClient;
import org.springframework.web.socket.sockjs.client.SockJsClient;
import org.springframework.web.socket.sockjs.client.Transport;
import org.springframework.web.socket.sockjs.client.WebSocketTransport;

import java.lang.reflect.Type;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;

public class WebSocketRemoteGateway implements RemoteGateway {

    private StompSessionHandler sessionHandler;
    private WebSocketStompClient stompClient;
    private StompSession stompSession;

    public WebSocketRemoteGateway(MessageConverter messageConverter) {

        WebSocketClient simpleWebSocketClient = new StandardWebSocketClient();
        List<Transport> transports = Collections.singletonList(new WebSocketTransport(simpleWebSocketClient));
        SockJsClient sockJsClient = new SockJsClient(transports);

        sessionHandler = new MyStompSessionHandler();
        stompClient = new WebSocketStompClient(sockJsClient);
        stompClient.setMessageConverter(messageConverter);
    }


    @Override
    public void connect(String endPoint) {
        try {
            stompSession = stompClient.connect(endPoint, sessionHandler).get();
        } catch (InterruptedException | ExecutionException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void disconnect() {
        stompSession.disconnect();
    }

    @Override
    public <T> void send(String destination, T object) {
        stompSession.send(destination, object);
    }

    @Override
    public <T, U> U sendAndReceive(String sendDestination, T object, String receiveQueue, Class<U> returnType) {

        Response<U> response = new Response<>();

        CountDownLatch latch = new CountDownLatch(1);

        StompSession.Subscription subscription = stompSession.subscribe(receiveQueue, new StompFrameHandler() {
            @Override
            public Type getPayloadType(StompHeaders stompHeaders) {
                return returnType;
            }

            @Override
            public void handleFrame(StompHeaders stompHeaders, Object o) {
                response.setValue(returnType.cast(o));
                latch.countDown();
            }
        });

        try {
            stompSession.send(sendDestination, object);
            latch.await();
            return response.value;
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        } finally {
            subscription.unsubscribe();
        }
    }

    private class MyStompSessionHandler extends StompSessionHandlerAdapter {
    }

    private class Response<T> {

        T value;

        void setValue(T value) {
            this.value = value;
        }
    }
}
