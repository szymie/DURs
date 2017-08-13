package org.szymie.server.strong.pessimistic;

import lsr.service.SerializableService;
import org.szymie.messages.BeginTransactionRequest;
import org.szymie.messages.BeginTransactionResponse;
import org.szymie.messages.Messages;

import java.util.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class BeginTransactionService extends SerializableService {

    private Messages.BeginTransactionRequest request;
    private Map<Long, TransactionMetadata> activeTransactions;
    private AtomicLong timestamp;

    public BeginTransactionService(Map<Long, TransactionMetadata> activeTransactions, AtomicLong timestamp) {
        this.activeTransactions = activeTransactions;
        this.timestamp = timestamp;
    }

    @Override
    protected Object execute(Object o) {

        System.err.println("REQUEST");


        request = (Messages.BeginTransactionRequest) o;
        TransactionMetadata newTransaction = new TransactionMetadata(request.getReadsMap().keySet(), request.getWritesMap().keySet());

        long newTransactionTimestamp = timestamp.get() + 1;
        activeTransactions.put(newTransactionTimestamp, newTransaction);

        synchronized (timestamp) {
            timestamp.set(newTransactionTimestamp);
            timestamp.notify();
        }

        System.err.println("activeTransactions: " + activeTransactions.size());

        for (Map.Entry<Long, TransactionMetadata> entry : activeTransactions.entrySet()) {

            if (!entry.getKey().equals(newTransactionTimestamp)) {

                TransactionMetadata transaction = entry.getValue();
                transaction.acquireReadLock();

                if (isAwaitingToStartNeeded(transaction)) {
                    newTransaction.getAwaitingToStart().add(entry.getKey());
                    transaction.getAwaitingForMe().add(newTransactionTimestamp);
                }

                if (isApplyingAfterNeeded(transaction)) {
                    newTransaction.setApplyAfter(entry.getKey());
                }

                transaction.releaseReadLock();
            }
        }

        System.err.println(newTransactionTimestamp + " can start " + newTransaction.getAwaitingToStart().isEmpty());

        newTransaction.getAwaitingToStart().forEach(transactionId -> {
            System.err.println(newTransactionTimestamp + " is waiting for " + transactionId);
        });

        return Messages.BeginTransactionResponse.newBuilder()
                .setTimestamp(newTransactionTimestamp)
                .setStartPossible(newTransaction.getAwaitingToStart().isEmpty())
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
        Map.Entry<Long, Map<Long, TransactionMetadata>> snapshot = (Map.Entry<Long, Map<Long, TransactionMetadata>>) o;
        snapshot.getValue().entrySet()
                .forEach(entry -> activeTransactions.put(entry.getKey(),
                        new TransactionMetadata(entry.getValue().getReads(),
                                entry.getValue().getWrites(),
                                entry.getValue().getAwaitingForMe(),
                                entry.getValue().getAwaitingToStart(),
                                entry.getValue().getApplyAfter(),
                                entry.getValue().isFinished()
                ) ));
        timestamp.set(snapshot.getKey());
        System.err.println("updateToSnapshot");
    }

    @Override
    protected Object makeObjectSnapshot() {
        System.err.println("makeObjectSnapshot");
        return new AbstractMap.SimpleEntry<>(timestamp.longValue(), activeTransactions);
    }
}
