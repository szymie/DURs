package org.szymie.server.strong.pessimistic;


import com.google.common.collect.TreeMultiset;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandler;
import org.szymie.BlockingMap;
import org.szymie.server.strong.ChannelInboundHandlerFactory;
import org.szymie.server.strong.optimistic.ResourceRepository;

import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Lock;

public class PessimisticServerChannelInboundHandlerFactory implements ChannelInboundHandlerFactory {

    private int id;
    private String paxosProcesses;
    private ResourceRepository resourceRepository;
    private AtomicLong timestamp;
    private BlockingMap<Long, BlockingQueue<ChannelHandlerContext>> contexts;
    private Map<Long, TransactionMetadata> activeTransactions;
    private GroupMessenger groupMessenger;

    private BlockingMap<Long, Boolean> activeTransactionFlags;

    private TreeMultiset<Long> liveTransactions;
    private Lock liveTransactionsLock;

    private AtomicLong lastCommitted;

    private int clientPoolSize;

    public PessimisticServerChannelInboundHandlerFactory(int id, String paxosProcesses, ResourceRepository resourceRepository, AtomicLong timestamp,
                                                         BlockingMap<Long, BlockingQueue<ChannelHandlerContext>> contexts, Map<Long, TransactionMetadata> activeTransactions,
                                                         BlockingMap<Long, Boolean> activeTransactionFlags,
                                                         TreeMultiset<Long> liveTransactions, Lock liveTransactionsLock,
                                                         AtomicLong lastCommitted, int clientPoolSize) {

        this.id = id;
        this.paxosProcesses = paxosProcesses;
        this.resourceRepository = resourceRepository;
        this.timestamp = timestamp;
        this.contexts = contexts;
        this.activeTransactions = activeTransactions;
        this.groupMessenger = groupMessenger;

        this.activeTransactionFlags = activeTransactionFlags;

        this.liveTransactions = liveTransactions;
        this.liveTransactionsLock = liveTransactionsLock;

        this.lastCommitted = lastCommitted;

        this.clientPoolSize = clientPoolSize;
    }

    @Override
    public ChannelInboundHandler create() {
        return new PessimisticServerMessageHandler(id, paxosProcesses, resourceRepository, contexts, activeTransactions, activeTransactionFlags, liveTransactions, liveTransactionsLock, lastCommitted, clientPoolSize);
    }
}
