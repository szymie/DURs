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

        Messages.ReadRequest readRequest = Messages.ReadRequest.newBuilder()
                .setKey(key)
                .setTimestamp(transactionData.timestamp)
                .build();

        Messages.Message message = Messages.Message.newBuilder()
                .setReadRequest(readRequest)
                .build();

        Messages.Message response = remoteGateway.sendAndReceive(message , Messages.Message.class);

        Messages.ReadResponse readResponse = response.getReadResponse();

        if(transactionData.timestamp == Long.MAX_VALUE) {
            transactionData.timestamp = readResponse.getTimestamp();
        }

        return new ReadResponse(readResponse.getValue(), readResponse.getTimestamp(), readResponse.getFresh());
    }
}
