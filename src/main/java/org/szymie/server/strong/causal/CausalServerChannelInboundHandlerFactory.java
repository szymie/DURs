package org.szymie.server.strong.causal;

import com.google.common.collect.TreeMultiset;
import io.netty.channel.ChannelInboundHandler;
import org.szymie.server.strong.ChannelInboundHandlerFactory;

import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Lock;

public class CausalServerChannelInboundHandlerFactory implements ChannelInboundHandlerFactory {

    private String paxosProcesses;
    private CausalResourceRepository resourceRepository;
    private AtomicLong timestamp;
    private TreeMultiset<Long> liveTransactions;
    private Lock liveTransactionsLock;
    private VectorClock vectorClock;

    public CausalServerChannelInboundHandlerFactory(String paxosProcesses, CausalResourceRepository resourceRepository, AtomicLong timestamp,
                                                        TreeMultiset<Long> liveTransactions, Lock liveTransactionsLock, VectorClock vectorClock) {
        this.paxosProcesses = paxosProcesses;
        this.resourceRepository = resourceRepository;
        this.timestamp = timestamp;
        this.liveTransactions = liveTransactions;
        this.liveTransactionsLock = liveTransactionsLock;
        this.vectorClock = vectorClock;
    }

    @Override
    public ChannelInboundHandler create() {
        return new CausalServerMessageHandler(paxosProcesses, resourceRepository, timestamp, liveTransactions, liveTransactionsLock, vectorClock);
    }
}
