package org.szymie.client.strong.pessimistic;

import org.springframework.messaging.converter.MessageConverter;
import org.springframework.messaging.simp.stomp.*;
import org.springframework.scheduling.concurrent.ConcurrentTaskScheduler;
import org.springframework.scheduling.concurrent.ThreadPoolTaskScheduler;
import org.springframework.web.socket.client.WebSocketClient;
import org.springframework.web.socket.client.standard.StandardWebSocketClient;
import org.springframework.web.socket.messaging.WebSocketStompClient;
import org.springframework.web.socket.sockjs.client.SockJsClient;
import org.springframework.web.socket.sockjs.client.Transport;
import org.springframework.web.socket.sockjs.client.WebSocketTransport;

import java.lang.reflect.Type;
import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.springframework.messaging.simp.stomp.StompSession.*;

public class WebSocketRemoteGateway implements RemoteGateway {

    private StompSessionHandler sessionHandler;
    private WebSocketStompClient stompClient;
    private DefaultStompSession stompSession;
    private CountDownLatch latch;
    private Semaphore semaphore;
    private Map<String, Response> responses;
    private Map<String, Subscription> subscriptions;

    private int reads;

    public WebSocketRemoteGateway(MessageConverter messageConverter) {

        WebSocketClient simpleWebSocketClient = new StandardWebSocketClient();
        List<Transport> transports = Collections.singletonList(new WebSocketTransport(simpleWebSocketClient));
        SockJsClient sockJsClient = new SockJsClient(transports);

        sessionHandler = new MyStompSessionHandler();
        stompClient = new WebSocketStompClient(sockJsClient);
        stompClient.setMessageConverter(messageConverter);

        responses = new HashMap<>();
        subscriptions = new HashMap<>();

        semaphore = new Semaphore(0);
    }


    @Override
    public void connect(String endPoint) {
        try {
            stompSession = (DefaultStompSession) stompClient.connect(endPoint, sessionHandler).get();


        } catch (InterruptedException | ExecutionException e) {
            throw new RuntimeException(e);
        }
    }

    public <T> void subscribe(String receiveQueue, Class<T> returnType) {

        CountDownLatch subscribeLatch = new CountDownLatch(1);

        Subscription subscription = stompSession.subscribe(receiveQueue, new StompFrameHandler() {
            @Override
            public Type getPayloadType(StompHeaders stompHeaders) {
                return returnType;
            }

            @Override
            public void handleFrame(StompHeaders stompHeaders, Object o) {

                if(subscribeLatch.getCount() == 0) {
                    setResponse(receiveQueue, new Response<>(returnType.cast(o)));
                    semaphore.release(1);
                } else {
                    subscribeLatch.countDown();
                }
            }
        });

        try {
            subscribeLatch.await();
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }

        subscriptions.put(receiveQueue, subscription);
    }

    private <T> void setResponse(String queue, Response<T> response) {
        responses.put(queue, response);
    }

    private <T> T getResponse(String queue, Class<T> returnType) {

        Response response = responses.get(queue);

        if(response == null) {
            throw new RuntimeException("response == null");
        }

        if(response.value == null) {
            return null;
        } else {
            return returnType.cast(response.value);
        }
    }

    @Override
    public void disconnect() {
        subscriptions.values().forEach(Subscription::unsubscribe);
        stompSession.disconnect();
    }

    @Override
    public <T> void send(String destination, T object) {
        stompSession.send(destination, object);
    }

    @Override
    public <T, U> U sendAndReceive(String sendDestination, T object, String receiveQueue, Class<U> returnType) {

        //latch = new CountDownLatch(1);

        while(true) {

            stompSession.send(sendDestination, object);

            try {
                semaphore.acquire(1);
                reads++;
                return getResponse(receiveQueue, returnType);
            } catch (InterruptedException e) {
                throw  new RuntimeException(e.getMessage() + "\n" + reads);
            }
        }
    }

    private class MyStompSessionHandler extends StompSessionHandlerAdapter {
    }

    private class Response<T> {

        T value;

        public Response() {
        }

        public Response(T value) {
            this.value = value;
        }

        void setValue(T value) {
            this.value = value;
        }
    }
}