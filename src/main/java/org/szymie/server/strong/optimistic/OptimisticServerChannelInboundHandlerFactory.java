package org.szymie.server.strong.optimistic;

import com.google.common.collect.TreeMultiset;
import io.netty.channel.ChannelInboundHandler;
import org.szymie.server.strong.ChannelInboundHandlerFactory;

import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Lock;

public class OptimisticServerChannelInboundHandlerFactory implements ChannelInboundHandlerFactory {

    private int id;
    private String paxosProcesses;
    private ResourceRepository resourceRepository;
    private AtomicLong timestamp;
    private TreeMultiset<Long> liveTransactions;
    private Lock liveTransactionsLock;

    public OptimisticServerChannelInboundHandlerFactory(int id, String paxosProcesses, ResourceRepository resourceRepository, AtomicLong timestamp,
                                                        TreeMultiset<Long> liveTransactions, Lock liveTransactionsLock) {
        this.id = id;
        this.paxosProcesses = paxosProcesses;
        this.resourceRepository = resourceRepository;
        this.timestamp = timestamp;
        this.liveTransactions = liveTransactions;
        this.liveTransactionsLock = liveTransactionsLock;
    }

    @Override
    public ChannelInboundHandler create() {
        return new OptimisticServerMessageHandler(id, paxosProcesses, resourceRepository, timestamp, liveTransactions, liveTransactionsLock);
    }
}
