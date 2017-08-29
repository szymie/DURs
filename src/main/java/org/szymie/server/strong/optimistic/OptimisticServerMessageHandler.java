package org.szymie.server.strong.optimistic;

import com.google.common.collect.TreeMultiset;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import org.szymie.messages.Messages;
import org.szymie.server.strong.BaseServerMessageHandler;

import java.util.Optional;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Lock;

public class OptimisticServerMessageHandler extends BaseServerMessageHandler {

    private TreeMultiset<Long> liveTransactions;
    private Lock liveTransactionsLock;

    public OptimisticServerMessageHandler(ResourceRepository resourceRepository, AtomicLong timestamp,
                                          TreeMultiset<Long> liveTransactions, Lock liveTransactionsLock) {
        super(resourceRepository, timestamp);
        this.liveTransactions = liveTransactions;
        this.liveTransactionsLock = liveTransactionsLock;
    }

    @Override
    public void channelRead0(ChannelHandlerContext ctx, Messages.Message msg) throws Exception {

        super.channelRead0(ctx, msg);

        switch (msg.getOneofMessagesCase()) {
            case READREQUEST:
                handleReadRequest(ctx, msg.getReadRequest());
                break;
            case COMMITREQUEST:
                handleCommitRequest(ctx, msg.getCommitRequest());
                break;
        }
    }

    private void handleCommitRequest(ChannelHandlerContext context, Messages.CommitRequest request) {

        liveTransactionsLock.lock();
        liveTransactions.remove(request.getTimestamp());
        liveTransactionsLock.unlock();

        Messages.CommitResponse commitResponse = Messages.CommitResponse.newBuilder().build();

        Messages.Message message = Messages.Message.newBuilder()
                .setCommitResponse(commitResponse)
                .build();

        context.writeAndFlush(message);
    }

    private void handleReadRequest(ChannelHandlerContext context, Messages.ReadRequest request) {

        boolean firstRead = request.getTimestamp() == Long.MAX_VALUE;

        long transactionTimestamp = firstRead ? timestamp.get() : request.getTimestamp();

        if(firstRead) {
            liveTransactionsLock.lock();
            liveTransactions.add(transactionTimestamp);
            liveTransactionsLock.unlock();
        }

        Optional<ValueWithTimestamp> valueOptional = resourceRepository.get(request.getKey(), transactionTimestamp);

        Messages.ReadResponse readResponse = valueOptional.map(valueWithTimestamp ->
                createReadResponse(valueWithTimestamp.value, transactionTimestamp, valueWithTimestamp.fresh))
                .orElse(createReadResponse("", transactionTimestamp, true));

        Messages.Message message = Messages.Message.newBuilder()
                .setReadResponse(readResponse)
                .build();

        context.writeAndFlush(message);
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
