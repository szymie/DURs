package org.szymie.server.strong.optimistic;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import org.szymie.messages.Messages;
import java.util.Optional;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.atomic.AtomicLong;

import static org.szymie.messages.Messages.Message;

public class OptimisticServerMessageHandler extends SimpleChannelInboundHandler<Message> {

    private ResourceRepository resourceRepository;
    private AtomicLong timestamp;
    private ConcurrentSkipListSet<Long> liveTransactions;

    public OptimisticServerMessageHandler(ResourceRepository resourceRepository, AtomicLong timestamp, ConcurrentSkipListSet<Long> liveTransactions) {
        this.resourceRepository = resourceRepository;
        this.timestamp = timestamp;
        this.liveTransactions = liveTransactions;
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

        boolean firstRead = request.getTimestamp() == Long.MAX_VALUE;

        long transactionTimestamp = firstRead ? timestamp.get() : request.getTimestamp();

        if(firstRead) {
            liveTransactions.add(transactionTimestamp);
        }

        Optional<ValueWithTimestamp> valueOptional = resourceRepository.get(request.getKey(), transactionTimestamp);

        Messages.ReadResponse readResponse = valueOptional.map(valueWithTimestamp ->
                createReadResponse(valueWithTimestamp.value, transactionTimestamp, valueWithTimestamp.fresh))
                .orElse(createReadResponse("", transactionTimestamp, true));

        Message response = Message.newBuilder()
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
