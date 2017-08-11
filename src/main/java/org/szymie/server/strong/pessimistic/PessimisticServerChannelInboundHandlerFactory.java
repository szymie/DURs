package org.szymie.server.strong.pessimistic;


import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandler;
import org.szymie.server.strong.ChannelInboundHandlerFactory;
import org.szymie.server.strong.optimistic.ResourceRepository;

import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

public class PessimisticServerChannelInboundHandlerFactory implements ChannelInboundHandlerFactory {

    private ResourceRepository resourceRepository;
    private AtomicLong timestamp;
    private Map<Long, ChannelHandlerContext> contexts;
    private Map<Long, TransactionMetadata> activeTransactions;
    private GroupMessenger groupMessenger;

    public PessimisticServerChannelInboundHandlerFactory(ResourceRepository resourceRepository, AtomicLong timestamp,
                                                         Map<Long, ChannelHandlerContext> contexts, Map<Long, TransactionMetadata> activeTransactions,
                                                         GroupMessenger groupMessenger) {
        this.resourceRepository = resourceRepository;
        this.timestamp = timestamp;
        this.contexts = contexts;
        this.activeTransactions = activeTransactions;
        this.groupMessenger = groupMessenger;
    }

    @Override
    public ChannelInboundHandler create() {
        return new PessimisticServerMessageHandler(resourceRepository, timestamp, contexts, activeTransactions, groupMessenger);
    }
}
