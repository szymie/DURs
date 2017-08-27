package org.szymie.server.strong.optimistic;

import com.google.common.collect.TreeMultiset;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import org.szymie.messages.Messages;
import java.util.Optional;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Lock;

import static org.szymie.messages.Messages.Message;

public class OptimisticServerMessageHandler extends SimpleChannelInboundHandler<Message> {

    private ResourceRepository resourceRepository;
    private AtomicLong timestamp;
    private TreeMultiset<Long> liveTransactions;
    private Lock liveTransactionsLock;

    public OptimisticServerMessageHandler(ResourceRepository resourceRepository, AtomicLong timestamp,
                                          TreeMultiset<Long> liveTransactions, Lock liveTransactionsLock) {
        this.resourceRepository = resourceRepository;
        this.timestamp = timestamp;
        this.liveTransactions = liveTransactions;
        this.liveTransactionsLock = liveTransactionsLock;
    }

    @Override
    public void channelRead0(ChannelHandlerContext ctx, Message msg) {

        System.err.println("msg " + msg);

        switch (msg.getOneofMessagesCase()) {
            case READREQUEST:
                handleReadRequest(ctx, msg.getReadRequest());
                break;
            case COMMITREQUEST:
                handleCommitRequest(ctx, msg.getCommitRequest());
                break;
            case INITREQUEST:
                handleInitRequest(ctx, msg.getInitRequest());
        }
    }

    private void handleInitRequest(ChannelHandlerContext context, Messages.InitRequest initRequest) {

        resourceRepository.clear();

        long timestamp = this.timestamp.incrementAndGet();
        initRequest.getWritesMap().forEach((key, value) ->  resourceRepository.put(key, value, timestamp));

        Messages.InitResponse initResponse = Messages.InitResponse.newBuilder()
                .build();

        Message response = Message.newBuilder()
                .setInitResponse(initResponse)
                .build();

        context.writeAndFlush(response);
    }

    private void handleCommitRequest(ChannelHandlerContext context, Messages.CommitRequest commitRequest) {

        liveTransactionsLock.lock();
        liveTransactions.remove(commitRequest.getTimestamp());
        liveTransactionsLock.unlock();

        Messages.CommitResponse commitResponse = Messages.CommitResponse.newBuilder().build();

        Message response = Message.newBuilder()
                .setCommitResponse(commitResponse)
                .build();

        context.writeAndFlush(response);
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
