package org.szymie.client.strong.optimistic;

import org.szymie.Configuration;
import org.szymie.messages.Messages;
import org.szymie.messages.ReadResponse;
import org.szymie.server.strong.optimistic.ValueWithTimestamp;

import java.util.Map;

public class NettyValueGateway extends BaseValueGateway {

    private NettyRemoteGateway remoteGateway;
    private Configuration configuration;

    public NettyValueGateway(NettyRemoteGateway remoteGateway) {
        this.remoteGateway = remoteGateway;
        configuration = new Configuration();
    }

    @Override
    public void openSession() {
        Map.Entry<Integer, String> replicaEndpoint = configuration.getRandomReplicaEndpoint();
        remoteGateway.connect(replicaEndpoint.getValue());
        sessionOpen = true;
    }

    @Override
    public void closeSession() {
        remoteGateway.disconnect();
        sessionOpen = false;
    }

    protected ReadResponse readRemotely(String key) {

        Messages.ReadRequest request = Messages.ReadRequest.newBuilder()
                .setKey(key)
                .setTimestamp(transactionData.timestamp)
                .build();

        Messages.ReadResponse response = remoteGateway.sendAndReceive(request, Messages.ReadResponse.class);

        if(transactionData.timestamp == Long.MAX_VALUE) {
            transactionData.timestamp = response.getTimestamp();
        }

        return new ReadResponse(response.getValue(), request.getTimestamp(), response.getFresh());
    }
}
