package org.szymie.server.strong.pessimistic;

import com.google.common.collect.Multiset;
import com.google.common.collect.TreeMultiset;
import io.netty.channel.ChannelHandlerContext;
import lsr.service.SerializableService;
import org.szymie.BlockingMap;
import org.szymie.messages.BeginTransactionResponse;
import org.szymie.messages.Messages;
import org.szymie.messages.StateUpdate;
import org.szymie.server.strong.optimistic.ResourceRepository;

import java.util.*;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Lock;

public class TransactionService extends SerializableService {

    private int id;
    private Messages.BeginTransactionRequest request;
    private Map<Long, TransactionMetadata> activeTransactions;
    private AtomicLong timestamp;
    private BlockingMap<Long, BlockingQueue<ChannelHandlerContext>> contexts;
    private BlockingMap<Long, Boolean> activeTransactionFlags;

    public TransactionService(int id, AtomicLong timestamp, Map<Long, TransactionMetadata> activeTransactions,
                              BlockingMap<Long, Boolean> activeTransactionFlags,
                              BlockingMap<Long, BlockingQueue<ChannelHandlerContext>> contexts) {
        this.id = id;
        this.timestamp = timestamp;
        this.activeTransactions = activeTransactions;
        this.activeTransactionFlags = activeTransactionFlags;
        this.contexts = contexts;
    }

    @Override
    protected Object execute(Object o) {

        System.err.println("REQUEST");

        Messages.Message message = (Messages.Message) o;

        switch (message.getOneofMessagesCase()) {
            case BEGINTRANSACTIONREQUEST:
                 return handleBeginTransaction(message.getBeginTransactionRequest());
            default:
                return new Object();
        }
    }

    private Messages.BeginTransactionResponse handleBeginTransaction(Messages.BeginTransactionRequest beginTransactionRequest) {

        request = beginTransactionRequest;

        TransactionMetadata newTransaction = new TransactionMetadata(beginTransactionRequest.getReadsMap().keySet(), beginTransactionRequest.getWritesMap().keySet());

        long newTransactionTimestamp = timestamp.incrementAndGet();

        System.err.println("activeTransactions: " + activeTransactions.size());

        if(id == beginTransactionRequest.getId()) {
            System.err.println("set context for in id: " + id + " where timestamp: " + newTransactionTimestamp);
            contexts.put(newTransactionTimestamp, new ArrayBlockingQueue<>(1));
        }

        boolean startPossible = true;

        for(Map.Entry<Long, TransactionMetadata> entry : activeTransactions.entrySet()) {

            TransactionMetadata transaction = entry.getValue();
            transaction.acquireReadLock();

            if(isAwaitingToStartNeeded(transaction)) {
                newTransaction.getAwaitingToStart().add(entry.getKey());
                transaction.getAwaitingForMe().add(newTransactionTimestamp);
                startPossible = false;
            }

            if(isApplyingAfterNeeded(transaction)) {
                newTransaction.setApplyAfter(entry.getKey());
            }

            transaction.releaseReadLock();
        }

        activeTransactions.put(newTransactionTimestamp, newTransaction);
        activeTransactionFlags.put(newTransactionTimestamp, startPossible);

        System.err.println(newTransactionTimestamp + " can start " + startPossible);
        newTransaction.getAwaitingToStart().forEach(transactionId -> System.err.println(newTransactionTimestamp + " is waiting for " + transactionId));

        return Messages.BeginTransactionResponse.newBuilder()
                .setStartPossible(startPossible)
                .setTimestamp(newTransactionTimestamp)
                .build();
    }

    private boolean isAwaitingToStartNeeded(TransactionMetadata transaction) {
        return !transaction.isFinished() && !Collections.disjoint(request.getReadsMap().keySet(), transaction.getWrites());
    }

    private boolean isApplyingAfterNeeded(TransactionMetadata transaction) {

        Set<String> readsAndWrites = new HashSet<>(transaction.getReads());
        readsAndWrites.addAll(transaction.getWrites());

        return !transaction.isFinished() && !Collections.disjoint(request.getWritesMap().keySet(), readsAndWrites);
    }

    @Override
    protected void updateToSnapshot(Object o) {
    }

    @Override
    protected Object makeObjectSnapshot() {
        return 1L;
    }
}
