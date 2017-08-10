package org.szymie.server.strong.pessimistic;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import lsr.paxos.client.ReplicationException;
import lsr.paxos.client.SerializableClient;
import org.szymie.messages.BeginTransactionResponse;
import org.szymie.messages.Messages;
import org.szymie.server.strong.optimistic.ResourceRepository;
import org.szymie.server.strong.optimistic.ValueWithTimestamp;

import java.io.IOException;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicLong;

public class PessimisticServerMessageHandler extends SimpleChannelInboundHandler<Messages.Message> {

    private ResourceRepository resourceRepository;
    private AtomicLong timestamp;
    private SerializableClient client;
    private Map<Long, ChannelHandlerContext> contexts;

    public PessimisticServerMessageHandler(ResourceRepository resourceRepository, AtomicLong timestamp, Map<Long, ChannelHandlerContext> contexts) {

        this.resourceRepository = resourceRepository;
        this.timestamp = timestamp;
        this.contexts = contexts;

        try {
            client = new SerializableClient(new lsr.common.Configuration(getClass().getResourceAsStream("paxos.properties")));
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
            case READREQUEST:
                handleReadRequest(ctx, msg.getReadRequest());
                break;
        }
    }

    private void handleBeginTransactionRequest(ChannelHandlerContext context, Messages.BeginTransactionRequest request) {

        client.connect();

        try {

            Messages.BeginTransactionResponse response = (Messages.BeginTransactionResponse) client.execute(request);

            if(response.getStartPossible()) {
                context.writeAndFlush(response);
            }

            contexts.put(response.getTimestamp(), context);
        } catch (IOException | ClassNotFoundException | ReplicationException e) {
            e.printStackTrace();
            throw new RuntimeException(e);
        }

    }

    private void handleReadRequest(ChannelHandlerContext context, Messages.ReadRequest request) {

        long transactionTimestamp = request.getTimestamp() == Long.MAX_VALUE ? timestamp.get() : request.getTimestamp();

        Optional<ValueWithTimestamp> valueOptional = resourceRepository.get(request.getKey(), transactionTimestamp);

        Messages.ReadResponse readResponse = valueOptional.map(valueWithTimestamp ->
                createReadResponse(valueWithTimestamp.value, transactionTimestamp, valueWithTimestamp.fresh))
                .orElse(createReadResponse("", transactionTimestamp, true));

        Messages.Message response = Messages.Message.newBuilder()
                .setReadResponse(readResponse)
                .build();

        context.writeAndFlush(response);
    }

    private Messages.ReadResponse createReadResponse(String value, long timestamp, boolean fresh) {
        return Messages.ReadResponse.newBuilder()
                .setValue(value)
                .setTimestamp(timestamp)
                .setFresh(fresh).build();
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
