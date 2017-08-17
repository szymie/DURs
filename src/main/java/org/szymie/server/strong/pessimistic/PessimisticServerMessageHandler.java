package org.szymie.server.strong.pessimistic;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import lsr.paxos.client.ReplicationException;
import lsr.paxos.client.SerializableClient;
import org.szymie.BlockingMap;
import org.szymie.messages.BeginTransactionResponse;
import org.szymie.messages.Messages;
import org.szymie.messages.StateUpdate;
import org.szymie.server.strong.optimistic.ResourceRepository;
import org.szymie.server.strong.optimistic.ValueWithTimestamp;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.atomic.AtomicLong;

public class PessimisticServerMessageHandler extends SimpleChannelInboundHandler<Messages.Message> {

    private int id;
    private ResourceRepository resourceRepository;
    private final AtomicLong timestamp;
    private SerializableClient client;
    private BlockingMap<Long, BlockingQueue<ChannelHandlerContext>> contexts;

    private Map<Long, TransactionMetadata> activeTransactions;

    private BlockingMap<Long, Boolean> activeTransactionFlags;

    public PessimisticServerMessageHandler(int id, ResourceRepository resourceRepository, AtomicLong timestamp,
                                           BlockingMap<Long, BlockingQueue<ChannelHandlerContext>> contexts,
                                           Map<Long, TransactionMetadata> activeTransactions,
                                           BlockingMap<Long, Boolean> activeTransactionFlags) {

        this.id = id;
        this.resourceRepository = resourceRepository;
        this.timestamp = timestamp;
        this.contexts = contexts;
        this.activeTransactions = activeTransactions;

        try {
            client = new SerializableClient(new lsr.common.Configuration(getClass().getClassLoader().getResourceAsStream("paxos.properties")));
            client.connect();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        this.activeTransactionFlags = activeTransactionFlags;
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


        try {

            Messages.BeginTransactionRequest requestWithId = Messages.BeginTransactionRequest.newBuilder(request)
                    .setId(id)
                    .build();

            Messages.BeginTransactionResponse response = (Messages.BeginTransactionResponse) client.execute(Messages.Message.newBuilder()
                    .setBeginTransactionRequest(requestWithId)
                    .build());

            System.err.println("for " + response.getTimestamp() + " context should have been set at "+ id);

            BlockingQueue<ChannelHandlerContext> contextHolder = contexts.get(response.getTimestamp());

            try {
                contextHolder.put(context);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }

            if(response.getStartPossible()) {

                Messages.Message message = Messages.Message.newBuilder()
                        .setBeginTransactionResponse(response)
                        .build();

                System.err.println("want to tell that " + response.getTimestamp() + " can start");
                context.writeAndFlush(message);
                System.err.println("told that " + response.getTimestamp() + " can start");
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

        activeTransactionFlags.get(request.getTimestamp());
        TransactionMetadata transaction = activeTransactions.get(request.getTimestamp());

        Messages.StateUpdateRequest stateUpdateRequest = Messages.StateUpdateRequest.newBuilder()
                .setTimestamp(request.getTimestamp())
                .setApplyAfter(transaction.getApplyAfter())
                .putAllWrites(request.getWritesMap())
                .build();

        Messages.Message message = Messages.Message
                .newBuilder()
                .setStateUpdateRequest(stateUpdateRequest)
                .build();

        System.err.println("timestamp: " + timestamp);
        System.err.println("transaction: " + transaction);

        try {
            client.execute(message);
        } catch (IOException | ClassNotFoundException | ReplicationException e) {
            e.printStackTrace();
        }
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
