package org.szymie.client.strong.causal;

import org.szymie.Configuration;
import org.szymie.client.strong.RemoteGateway;
import org.szymie.messages.CausalReadResponse;
import org.szymie.messages.Messages;
import org.szymie.messages.ReadResponse;

import java.util.Arrays;
import java.util.Map;

public class NettyCausalValueGateway extends BaseValueGateway {

    private Session session;
    private RemoteGateway remoteGateway;
    private Configuration configuration;

    public NettyCausalValueGateway(Session session, RemoteGateway remoteGateway) {
        this.session = session;
        this.remoteGateway = remoteGateway;
        configuration = new Configuration();
    }

    public NettyCausalValueGateway(Session session, RemoteGateway remoteGateway, Configuration configuration) {
        this.session = session;
        this.remoteGateway = remoteGateway;
        this.configuration = configuration;
    }

    public void openSession() {
        Map.Entry<Integer, String> replicaEndpoint = configuration.getRandomReplicaEndpoint();
        remoteGateway.connect(replicaEndpoint.getValue());
        sessionOpen = true;
    }

    public void closeSession() {
        remoteGateway.disconnect();
        sessionOpen = false;
    }

    protected CausalReadResponse readRemotely(String key) {

        Messages.ReadRequest readRequest = Messages.ReadRequest.newBuilder()
                .setKey(key)
                .setTimestamp(transactionData.timestamp)
                .setLocalClock(session.localClock)
                .build();

        Messages.Message message = Messages.Message.newBuilder()
                .setReadRequest(readRequest)
                .build();

        Messages.ReadResponse causalReadResponse = remoteGateway.sendAndReceive(message , Messages.ReadResponse.class);

        if(transactionData.timestamp == Long.MAX_VALUE) {
            transactionData.timestamp = causalReadResponse.getTimestamp();
            session.localClock = Math.max(session.localClock, transactionData.timestamp);
        }

        String value = causalReadResponse.getValue();

        return new CausalReadResponse(Arrays.asList(value), causalReadResponse.getTimestamp(), causalReadResponse.getFresh());
    }
}
