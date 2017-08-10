package org.szymie.client.strong.pessimistic;

public interface RemoteGateway {

    void connect(String endPoint);
    void disconnect();
    <T> void send(T object);
    <T, U> U sendAndReceive(T object, Class<U> returnType);
}
