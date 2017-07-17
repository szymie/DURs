package org.szymie.client.strong.pessimistic;

import org.szymie.Configuration;
import org.szymie.client.strong.optimistic.TransactionData;
import org.szymie.client.strong.optimistic.ValueGateway;

import java.util.Map;

public class WebSocketValueGateway implements ValueGateway {

    private RemoteGateway remoteGateway;
    private Configuration configuration;
    private boolean sessionOpen;

    public WebSocketValueGateway(RemoteGateway remoteGateway) {
        this.remoteGateway = remoteGateway;
        configuration = new Configuration();
    }

    @Override
    public void openSession() {
        Map.Entry<Integer, String> replicaEndpoint = configuration.getRandomReplicaEndpoint();
        remoteGateway.connect("ws://" + replicaEndpoint.getValue() + "/replica");
        sessionOpen = true;
    }

    @Override
    public void closeSession() {
        sessionOpen = false;
        remoteGateway.disconnect();
    }

    @Override
    public boolean isSessionOpen() {
        return sessionOpen;
    }

    @Override
    public String read(String key) {
        return null;
    }

    @Override
    public void write(String key, String value) {

    }

    @Override
    public void remove(String key) {

    }

    @Override
    public void clear() {

    }

    @Override
    public TransactionData getTransactionData() {
        return null;
    }
}
