package org.szymie.server.strong.causal;

import com.google.common.collect.TreeMultiset;
import io.netty.channel.ChannelInboundHandler;
import org.szymie.BlockingMap;
import org.szymie.server.strong.ChannelInboundHandlerFactory;

import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Lock;

public class CausalServerChannelInboundHandlerFactory implements ChannelInboundHandlerFactory {

    private int id;
    private String paxosProcesses;
    private CausalResourceRepository resourceRepository;
    private AtomicLong timestamp;
    private TreeMultiset<Long> liveTransactions;
    private Lock liveTransactionsLock;
    private VectorClock vectorClock;
    private BlockingMap<Long, Long> responses;
    private int clientPoolSize;

    public CausalServerChannelInboundHandlerFactory(int id, String paxosProcesses, CausalResourceRepository resourceRepository, AtomicLong timestamp,
                                                    TreeMultiset<Long> liveTransactions, Lock liveTransactionsLock, VectorClock vectorClock,
                                                    BlockingMap<Long, Long> responses, int clientPoolSize) {
        this.id = id;
        this.paxosProcesses = paxosProcesses;
        this.resourceRepository = resourceRepository;
        this.timestamp = timestamp;
        this.liveTransactions = liveTransactions;
        this.liveTransactionsLock = liveTransactionsLock;
        this.vectorClock = vectorClock;
        this.responses = responses;
        this.clientPoolSize = clientPoolSize;
    }

    @Override
    public ChannelInboundHandler create() {
        return new CausalServerMessageHandler(id, paxosProcesses, resourceRepository, timestamp, liveTransactions, liveTransactionsLock, vectorClock, responses, clientPoolSize);
    }
}
