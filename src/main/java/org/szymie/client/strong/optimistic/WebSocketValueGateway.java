package org.szymie.client.strong.optimistic;


import org.szymie.Configuration;
import org.szymie.client.strong.pessimistic.RemoteGateway;
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
        sessionOpen = false;
    }


    @Override
    public void openSession() {
        Map.Entry<Integer, String> replicaEndpoint = configuration.getRandomReplicaEndpoint();
        remoteGateway.connect("ws://" + /*"127.0.0.1:8080"*/ replicaEndpoint.getValue() + "/replica");
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

    public String read(String key) {

        if(key == null) {
            throw new RuntimeException("empty key cannot be read");
        }

        ValueWithTimestamp value = transactionData.writtenValues.get(key);

        if(value == null) {

            value = transactionData.readValues.get(key);

            if(value == null) {

                ReadResponse readResponse = readRemotely(key);
                value = new ValueWithTimestamp(readResponse.value, readResponse.timestamp, readResponse.fresh);
            }
        }

        transactionData.readValues.put(key, value);

        return value.value;
    }

    private ReadResponse readRemotely(String key) {

        ReadRequest request = new ReadRequest(key, transactionData.timestamp);

        ReadResponse response = remoteGateway.sendAndReceive("/replica/read",
                request, "/user/queue/read-response", ReadResponse.class);

        if(transactionData.timestamp == Long.MAX_VALUE) {
            transactionData.timestamp = response.timestamp;
        }

        return response;
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
