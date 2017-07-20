package org.szymie.client.strong.pessimistic;

import org.szymie.Configuration;
import org.szymie.client.strong.optimistic.TransactionData;
import org.szymie.client.strong.optimistic.ValueGateway;
import org.szymie.messages.ReadRequest;
import org.szymie.messages.ReadResponse;
import org.szymie.server.strong.optimistic.ValueWithTimestamp;

import java.util.Map;

public class WebSocketValueGateway implements ValueGateway {

    private RemoteGateway remoteGateway;
    private TransactionData transactionData;
    private Configuration configuration;
    private boolean sessionOpen;

    public WebSocketValueGateway(RemoteGateway remoteGateway) {
        this.remoteGateway = remoteGateway;
        transactionData = new TransactionData();
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
        remoteGateway.disconnect();
        sessionOpen = false;
    }

    @Override
    public boolean isSessionOpen() {
        return sessionOpen;
    }

    @Override
    public String read(String key) {

        if(key == null) {
            throw new RuntimeException("empty key cannot be read");
        }

        ValueWithTimestamp value = transactionData.writtenValues.get(key);

        if(value == null) {

            value = transactionData.readValues.get(key);

            if(value == null) {
                ReadRequest request = new ReadRequest(key, transactionData.timestamp);

                ReadResponse response = remoteGateway.sendAndReceive("/replica/read",
                        request, "/replica/queue/read-response", ReadResponse.class);

                value = new ValueWithTimestamp(response.value, response.timestamp, response.fresh);
            }
        }

        transactionData.readValues.put(key, value);

        return value.value;
    }

    @Override
    public void write(String key, String value) {

        if(key == null) {
            throw new RuntimeException("empty key cannot be written");
        }

        if(value == null) {
            throw new RuntimeException("empty value cannot be written");
        }

        transactionData.writtenValues.put(key, new ValueWithTimestamp(value, Long.MAX_VALUE, true));
    }

    @Override
    public void remove(String key) {

        if(key == null) {
            throw new RuntimeException("empty key cannot be written");
        }

        transactionData.writtenValues.put(key, new ValueWithTimestamp(null, Long.MAX_VALUE, true));
    }

    @Override
    public void clear() {
        transactionData.clear();
    }

    @Override
    public TransactionData getTransactionData() {
        return transactionData;
    }
}
