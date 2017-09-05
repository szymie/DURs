package org.szymie.client.strong.causal;

import org.szymie.Configuration;
import org.szymie.client.strong.RemoteGateway;
import org.szymie.messages.CausalReadResponse;
import org.szymie.messages.Messages;
import org.szymie.messages.ReadResponse;
import java.util.Map;

public class NettyCausalValueGateway extends BaseValueGateway {

    private RemoteGateway remoteGateway;
    private Configuration configuration;

    public NettyCausalValueGateway(RemoteGateway remoteGateway) {
        this.remoteGateway = remoteGateway;
        configuration = new Configuration();
    }

    public NettyCausalValueGateway(RemoteGateway remoteGateway, Configuration configuration) {
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
                .build();

        Messages.Message message = Messages.Message.newBuilder()
                .setReadRequest(readRequest)
                .build();

        Messages.CausalReadResponse causalReadResponse = remoteGateway.sendAndReceive(message , Messages.CausalReadResponse.class);

        if(transactionData.timestamp == Long.MAX_VALUE) {
            transactionData.timestamp = causalReadResponse.getTimestamp();
        }

        return new CausalReadResponse(causalReadResponse.getValuesList(), causalReadResponse.getTimestamp(), causalReadResponse.getFresh());
    }
}
