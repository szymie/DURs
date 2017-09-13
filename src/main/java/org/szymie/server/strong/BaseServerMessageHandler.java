package org.szymie.server.strong;

import com.google.common.collect.TreeMultiset;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import org.szymie.messages.Messages;
import org.szymie.server.strong.optimistic.ResourceRepository;
import org.szymie.server.strong.optimistic.ValueWithTimestamp;

import java.util.Optional;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Lock;

public abstract class BaseServerMessageHandler extends SimpleChannelInboundHandler<Messages.Message> {

    protected ResourceRepository resourceRepository;
    protected AtomicLong timestamp;

    protected TreeMultiset<Long> liveTransactions;
    protected Lock liveTransactionsLock;

    public BaseServerMessageHandler(ResourceRepository resourceRepository, AtomicLong timestamp, TreeMultiset<Long> liveTransactions, Lock liveTransactionsLock) {
        this.resourceRepository = resourceRepository;
        this.timestamp = timestamp;
        this.liveTransactions = liveTransactions;
        this.liveTransactionsLock = liveTransactionsLock;
    }

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, Messages.Message msg) throws Exception {

        switch (msg.getOneofMessagesCase()) {
            case INITREQUEST:
                handleInitRequest(ctx, msg.getInitRequest());
                break;
        }
    }

    private void handleInitRequest(ChannelHandlerContext context, Messages.InitRequest initRequest) {

        resourceRepository.clear();

        long time = timestamp.incrementAndGet();
        initRequest.getWritesMap().forEach((key, value) -> resourceRepository.put(key, value, time));

        Messages.InitResponse initResponse = Messages.InitResponse.newBuilder()
                .build();

        Messages.Message response = Messages.Message.newBuilder()
                .setInitResponse(initResponse)
                .build();

        context.writeAndFlush(response);
    }

    protected void handleReadRequest(ChannelHandlerContext context, Messages.ReadRequest request) {

        boolean firstRead = request.getTimestamp() == Long.MAX_VALUE;

        long transactionTimestamp = firstRead ? timestamp.get() : request.getTimestamp();

        if(firstRead) {
            liveTransactionsLock.lock();
            liveTransactions.add(transactionTimestamp);
            liveTransactionsLock.unlock();
        }

        Optional<ValueWithTimestamp<String>> valueOptional = resourceRepository.get(request.getKey(), transactionTimestamp);

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

    protected void commitReadOnlyTransaction(ChannelHandlerContext context, Messages.CommitRequest request) {

        liveTransactionsLock.lock();
        liveTransactions.remove(request.getTimestamp());
        liveTransactionsLock.unlock();

        Messages.CommitResponse response = Messages.CommitResponse.newBuilder().build();
        Messages.Message message = Messages.Message.newBuilder().setCommitResponse(response).build();

        context.writeAndFlush(message);
    }
}
