package org.szymie.client.strong.pessimistic;

public interface RemoteGateway {

    void connect(String endPoint);
    void disconnect();
    <T> void subscribe(String receiveQueue, Class<T> returnType);
    <T> void send(String destination, T object);
    <T, U> U sendAndReceive(String sendDestination, T object, String receiveQueue, Class<U> returnType);
}
