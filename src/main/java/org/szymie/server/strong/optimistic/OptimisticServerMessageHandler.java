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

    public OptimisticServerMessageHandler(ResourceRepository resourceRepository, AtomicLong timestamp,
                                          TreeMultiset<Long> liveTransactions, Lock liveTransactionsLock) {
        super(resourceRepository, timestamp, liveTransactions, liveTransactionsLock);
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
        commitReadOnlyTransaction(context, request);
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        cause.printStackTrace();
        ctx.close();
    }
}
