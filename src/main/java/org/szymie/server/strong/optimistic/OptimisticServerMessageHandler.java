package org.szymie.server.strong.optimistic;

import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import org.szymie.messages.Messages;
import org.szymie.server.strong.optimistic.ResourceRepository;

import java.util.Optional;
import java.util.concurrent.atomic.AtomicLong;

import static org.szymie.messages.Messages.Message;

public class OptimisticServerMessageHandler extends SimpleChannelInboundHandler<Message> {

    private ResourceRepository resourceRepository;
    private AtomicLong timestamp;

    public OptimisticServerMessageHandler(ResourceRepository resourceRepository, AtomicLong timestamp) {
        this.resourceRepository = resourceRepository;
        this.timestamp = timestamp;
    }

    @Override
    public void channelRead0(ChannelHandlerContext ctx, Message msg) {

        System.err.println("msg " + msg);

        switch (msg.getOneofMessagesCase()) {
            case READREQUEST:
                handleReadRequest(ctx, msg.getReadRequest());
                break;
        }
    }

    private void handleReadRequest(ChannelHandlerContext context, Messages.ReadRequest request) {

        long transactionTimestamp = request.getTimestamp() == Long.MAX_VALUE ? timestamp.get() : request.getTimestamp();

        Optional<ValueWithTimestamp> valueOptional = resourceRepository.get(request.getKey(), transactionTimestamp);

        Messages.ReadResponse readResponse = valueOptional.map(valueWithTimestamp ->
                createReadResponse(valueWithTimestamp.value, transactionTimestamp, valueWithTimestamp.fresh))
                .orElse(createReadResponse("", transactionTimestamp, true));

        Message response = Message.newBuilder()
                .setReadResponse(readResponse)
                .build();

        context.writeAndFlush(response);

        System.err.println("written");
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
