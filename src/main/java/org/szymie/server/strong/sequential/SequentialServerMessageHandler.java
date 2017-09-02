package org.szymie.server.strong.sequential;

import io.netty.channel.ChannelHandlerContext;
import lsr.common.PID;
import lsr.paxos.client.ReplicationException;
import lsr.paxos.client.SerializableClient;
import org.szymie.PaxosProcessesCreator;
import org.szymie.messages.Messages;
import org.szymie.server.strong.BaseServerMessageHandler;
import org.szymie.server.strong.optimistic.ResourceRepository;

import java.io.IOException;
import java.io.InputStream;
import java.util.List;

public class SequentialServerMessageHandler extends BaseServerMessageHandler implements PaxosProcessesCreator {

    private SerializableClient client;

    public SequentialServerMessageHandler(String paxosProcesses) {

        super(null, null, null, null);

        List<PID> processes = createPaxosProcesses(paxosProcesses);

        InputStream paxosProperties = getClass().getClassLoader().getResourceAsStream("paxos.properties");

        try {

            if(processes.isEmpty()) {
                client = new SerializableClient(new lsr.common.Configuration(paxosProperties));
            } else {
                client = new SerializableClient(new lsr.common.Configuration(processes, paxosProperties));
            }

            client.connect();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void channelRead0(ChannelHandlerContext ctx, Messages.Message msg) throws Exception {

        super.channelRead0(ctx, msg);

        switch (msg.getOneofMessagesCase()) {
            case TRANSACTIONEXECUTIONREQUEST:
                handleTransactionExecutionRequest(ctx, msg.getTransactionExecutionRequest());
                break;
        }
    }

    private void handleTransactionExecutionRequest(ChannelHandlerContext context, Messages.TransactionExecutionRequest request) {

        try {

            Messages.TransactionExecutionResponse transactionExecutionResponse = (Messages.TransactionExecutionResponse) client.execute(request);

            Messages.Message message = Messages.Message.newBuilder()
                    .setTransactionExecutionResponse(transactionExecutionResponse)
                    .build();

            context.writeAndFlush(message);
        } catch (IOException | ClassNotFoundException | ReplicationException e) {
            e.printStackTrace();
            throw new RuntimeException(e);
        }
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        cause.printStackTrace();
        ctx.close();
    }
}
