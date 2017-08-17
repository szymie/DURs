package org.szymie.server.strong.pessimistic;


import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandler;
import org.szymie.BlockingMap;
import org.szymie.server.strong.ChannelInboundHandlerFactory;
import org.szymie.server.strong.optimistic.ResourceRepository;

import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.atomic.AtomicLong;

public class PessimisticServerChannelInboundHandlerFactory implements ChannelInboundHandlerFactory {

    private int id;
    private ResourceRepository resourceRepository;
    private AtomicLong timestamp;
    private BlockingMap<Long, BlockingQueue<ChannelHandlerContext>> contexts;
    private Map<Long, TransactionMetadata> activeTransactions;
    private GroupMessenger groupMessenger;

    private BlockingMap<Long, Boolean> activeTransactionFlags;

    public PessimisticServerChannelInboundHandlerFactory(int id, ResourceRepository resourceRepository, AtomicLong timestamp,
                                                         BlockingMap<Long, BlockingQueue<ChannelHandlerContext>> contexts, Map<Long, TransactionMetadata> activeTransactions,
                                                         BlockingMap<Long, Boolean> activeTransactionFlags) {

        this.id = id;
        this.resourceRepository = resourceRepository;
        this.timestamp = timestamp;
        this.contexts = contexts;
        this.activeTransactions = activeTransactions;
        this.groupMessenger = groupMessenger;

        this.activeTransactionFlags = activeTransactionFlags;
    }

    @Override
    public ChannelInboundHandler create() {
        return new PessimisticServerMessageHandler(id, resourceRepository, timestamp, contexts, activeTransactions, activeTransactionFlags);
    }
}
