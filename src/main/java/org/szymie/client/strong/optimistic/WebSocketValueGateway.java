package org.szymie.client.strong.optimistic;


import org.szymie.Configuration;
import org.szymie.client.strong.pessimistic.RemoteGateway;
import org.szymie.client.strong.pessimistic.WebSocketRemoteGateway;
import org.szymie.messages.ReadRequest;
import org.szymie.messages.ReadResponse;
import org.szymie.server.strong.optimistic.ValueWithTimestamp;

import java.util.Map;

public class WebSocketValueGateway extends BaseValueGateway {

    private WebSocketRemoteGateway remoteGateway;
    private Configuration configuration;

    public WebSocketValueGateway(WebSocketRemoteGateway remoteGateway) {
        this.remoteGateway = remoteGateway;
        configuration = new Configuration();
    }

    @Override
    public void openSession() {
        Map.Entry<Integer, String> replicaEndpoint = configuration.getRandomReplicaEndpoint();
        remoteGateway.connect("ws://" + /*"127.0.0.1:8080"*/ replicaEndpoint.getValue() + "/replica");
        remoteGateway.subscribe("/user/queue/read-response", ReadResponse.class);
        sessionOpen = true;
    }

    @Override
    public void closeSession() {
        remoteGateway.disconnect();
        sessionOpen = false;
    }

    protected ReadResponse readRemotely(String key) {

        ReadRequest request = new ReadRequest(key, transactionData.timestamp);

        ReadResponse response = remoteGateway.sendAndReceive("/replica/read",
                request, "/user/queue/read-response", ReadResponse.class);

        if(transactionData.timestamp == Long.MAX_VALUE) {
            transactionData.timestamp = response.timestamp;
        }

        return response;
    }
}
