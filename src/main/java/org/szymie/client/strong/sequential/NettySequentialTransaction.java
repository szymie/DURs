package org.szymie.client.strong.sequential;



        import org.szymie.Configuration;
        import org.szymie.client.strong.RemoteGateway;
        import org.szymie.client.strong.optimistic.*;
        import org.szymie.messages.*;

        import java.util.ArrayList;
        import java.util.HashMap;
        import java.util.List;
        import java.util.Map;
        import java.util.stream.Collectors;

public class NettySequentialTransaction implements Transaction {

    private RemoteGateway remoteGateway;

    private List<String> reads;
    private List<String> writes;

    private Configuration configuration;

    public NettySequentialTransaction() {
        this(new Configuration());
    }

    public NettySequentialTransaction(Configuration configuration) {
        this(0, configuration);
    }

    public NettySequentialTransaction(int numberOfClientThreads, Configuration configuration) {
        remoteGateway = new NettyRemoteGateway(numberOfClientThreads, new ClientChannelInitializer(new SequentialClientMessageHandlerFactory()));
        reads = new ArrayList<>();
        writes = new ArrayList<>();
        this.configuration = configuration;
    }

    public long getTimestamp() {
        return 0;
    }

    @Override
    public void begin() {
    }

    @Override
    public String read(String key) {
        reads.add(key);
        return "";
    }

    @Override
    public void write(String key, String value) {
        writes.add(key);
    }

    @Override
    public void remove(String key) {
    }

    @Override
    public boolean commit() {

        Map.Entry<Integer, String> replicaEndpoint = configuration.getRandomReplicaEndpoint();
        remoteGateway.connect(replicaEndpoint.getValue());

        Messages.TransactionExecutionRequest request = Messages.TransactionExecutionRequest.newBuilder()
                .addAllReads(reads)
                .addAllWrites(writes)
                .build();

        Messages.Message message = Messages.Message.newBuilder()
                .setTransactionExecutionRequest(request)
                .build();

        remoteGateway.sendAndReceive(message, Messages.TransactionExecutionResponse.class);

        remoteGateway.disconnect();

        return true;
    }
}
