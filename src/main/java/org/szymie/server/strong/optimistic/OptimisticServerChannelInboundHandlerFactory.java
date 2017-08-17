package org.szymie.server.strong.optimistic;

import io.netty.channel.ChannelInboundHandler;
import org.szymie.server.strong.ChannelInboundHandlerFactory;

import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.atomic.AtomicLong;

public class OptimisticServerChannelInboundHandlerFactory implements ChannelInboundHandlerFactory {

    private ResourceRepository resourceRepository;
    private AtomicLong timestamp;
    private ConcurrentSkipListSet<Long> liveTransactions;

    public OptimisticServerChannelInboundHandlerFactory(ResourceRepository resourceRepository, AtomicLong timestamp, ConcurrentSkipListSet<Long> liveTransactions) {
        this.resourceRepository = resourceRepository;
        this.timestamp = timestamp;
        this.liveTransactions = liveTransactions;
    }

    @Override
    public ChannelInboundHandler create() {
        return new OptimisticServerMessageHandler(resourceRepository, timestamp, liveTransactions);
    }
}
