package org.szymie.server.strong.pessimistic;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import lsr.paxos.client.ReplicationException;
import lsr.paxos.client.SerializableClient;
import org.szymie.messages.BeginTransactionResponse;
import org.szymie.messages.Messages;
import org.szymie.messages.StateUpdate;
import org.szymie.server.strong.optimistic.ResourceRepository;
import org.szymie.server.strong.optimistic.ValueWithTimestamp;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicLong;

public class PessimisticServerMessageHandler extends SimpleChannelInboundHandler<Messages.Message> {

    private ResourceRepository resourceRepository;
    private final AtomicLong timestamp;
    private SerializableClient client;
    private Map<Long, ChannelHandlerContext> contexts;

    private Map<Long, TransactionMetadata> activeTransactions;
    private GroupMessenger groupMessenger;

    public PessimisticServerMessageHandler(ResourceRepository resourceRepository, AtomicLong timestamp, Map<Long, ChannelHandlerContext> contexts,
                                           Map<Long, TransactionMetadata> activeTransactions, GroupMessenger groupMessenger) {

        this.resourceRepository = resourceRepository;
        this.timestamp = timestamp;
        this.contexts = contexts;
        this.activeTransactions = activeTransactions;
        this.groupMessenger = groupMessenger;

        try {
            client = new SerializableClient(new lsr.common.Configuration(getClass().getClassLoader().getResourceAsStream("paxos.properties")));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void channelRead0(ChannelHandlerContext ctx, Messages.Message msg) {

        System.err.println("msg " + msg);

        switch (msg.getOneofMessagesCase()) {
            case BEGINTRANSACTIONREQUEST:
                handleBeginTransactionRequest(ctx, msg.getBeginTransactionRequest());
                break;
            case READREQUEST:
                System.err.println("Read request from " + msg.getReadRequest().getTimestamp());
                handleReadRequest(ctx, msg.getReadRequest());
                break;
            case COMMITREQUEST:
                handleCommitRequest(ctx, msg.getCommitRequest());
                break;
        }
    }

    private void handleBeginTransactionRequest(ChannelHandlerContext context, Messages.BeginTransactionRequest request) {

        client.connect();

        try {

            Messages.BeginTransactionResponse response = (Messages.BeginTransactionResponse) client.execute(request);

            contexts.put(response.getTimestamp(), context);

            if(response.getStartPossible()) {

                Messages.Message message = Messages.Message.newBuilder()
                        .setBeginTransactionResponse(response)
                        .build();

                context.writeAndFlush(message);
            }
        } catch (IOException | ClassNotFoundException | ReplicationException e) {
            e.printStackTrace();
            throw new RuntimeException(e);
        }
    }

    private void handleReadRequest(ChannelHandlerContext context, Messages.ReadRequest request) {

        long transactionTimestamp = request.getTimestamp() == Long.MAX_VALUE ? timestamp.get() : request.getTimestamp();

        Optional<ValueWithTimestamp> valueOptional = resourceRepository.get(request.getKey(), transactionTimestamp);

        Messages.ReadResponse response = valueOptional.map(valueWithTimestamp ->
                createReadResponse(valueWithTimestamp.value, transactionTimestamp, valueWithTimestamp.fresh))
                .orElse(createReadResponse("", transactionTimestamp, true));

        Messages.Message message = Messages.Message.newBuilder()
                .setReadResponse(response)
                .build();

        context.writeAndFlush(message);
    }

    private Messages.ReadResponse createReadResponse(String value, long timestamp, boolean fresh) {
        return Messages.ReadResponse.newBuilder()
                .setValue(value)
                .setTimestamp(timestamp)
                .setFresh(fresh).build();
    }

    private void handleCommitRequest(ChannelHandlerContext context, Messages.CommitRequest request) {

        try {
            synchronized(timestamp) {
                while(timestamp.get() < request.getTimestamp()) {
                    timestamp.wait();
                }
            }
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }

        TransactionMetadata transaction = activeTransactions.get(request.getTimestamp());

        long timestamp = request.getTimestamp();

        System.err.println("timestamp: " + timestamp);
        System.err.println("transaction: " + transaction);

        groupMessenger.send(new StateUpdate(request.getTimestamp(), transaction.getApplyAfter(), new HashMap<>(request.getWritesMap())));
    }


    /*@Override
    public void channelActive(ChannelHandlerContext ctx) throws Exception {

        ByteBuf time = ctx.alloc().buffer(4);
        time.writeInt((int) (System.currentTimeMillis() / 1000L + 2208988800L));

        ChannelFuture f = ctx.writeAndFlush(time);

        //f.sync();

        //ctx.close();

        f.addListener(ChannelFutureListener.CLOSE);

        f.addListener((ChannelFutureListener) future -> {
            assert f == future;
            ctx.close();
        });
    }*/



    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        cause.printStackTrace();
        ctx.close();
    }

}
