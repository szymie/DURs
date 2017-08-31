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
    private long lastApplied;
    private ResourceRepository resourceRepository;

    private Set<StateUpdate> waitingUpdates;

    private BlockingMap<Long, BlockingQueue<ChannelHandlerContext>> contexts;

    private BlockingMap<Long, Boolean> activeTransactionFlags;

    private TreeMultiset<Long> liveTransactions;
    private Lock liveTransactionsLock;

    public TransactionService(int id, ResourceRepository resourceRepository, AtomicLong timestamp, Map<Long, TransactionMetadata> activeTransactions,
                              BlockingMap<Long, Boolean> activeTransactionFlags,
                              BlockingMap<Long, BlockingQueue<ChannelHandlerContext>> contexts,
                              TreeMultiset<Long> liveTransactions, Lock liveTransactionsLock) {
        this.id = id;
        this.resourceRepository = resourceRepository;
        this.timestamp = timestamp;
        this.activeTransactions = activeTransactions;
        this.activeTransactionFlags = activeTransactionFlags;
        this.contexts = contexts;
        this.liveTransactions = liveTransactions;
        this.liveTransactionsLock = liveTransactionsLock;

        lastApplied = 0;
        waitingUpdates = new TreeSet<>();
    }

    @Override
    protected Object execute(Object o) {

        System.err.println("REQUEST");

        Messages.Message message = (Messages.Message) o;

        switch (message.getOneofMessagesCase()) {
            case BEGINTRANSACTIONREQUEST:
                 return handleBeginTransaction(message.getBeginTransactionRequest());
            case STATEUPDATEREQUEST:
                handleStateUpdateRequest(message.getStateUpdateRequest());
                return Messages.StateUpdateResponse.newBuilder().build();
            default:
                return new Object();
        }
    }

    private BeginTransactionResponse handleBeginTransaction(Messages.BeginTransactionRequest beginTransactionRequest) {

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

        newTransaction.getAwaitingToStart().forEach(transactionId -> {
            System.err.println(newTransactionTimestamp + " is waiting for " + transactionId);
        });

        return new BeginTransactionResponse(newTransactionTimestamp, startPossible);
    }

    private boolean isAwaitingToStartNeeded(TransactionMetadata transaction) {
        return !transaction.isFinished() && !Collections.disjoint(request.getReadsMap().keySet(), transaction.getWrites());
    }

    private boolean isApplyingAfterNeeded(TransactionMetadata transaction) {

        Set<String> readsAndWrites = new HashSet<>(transaction.getReads());
        readsAndWrites.addAll(transaction.getWrites());

        return !transaction.isFinished() && !Collections.disjoint(request.getWritesMap().keySet(), readsAndWrites);
    }

    private void handleStateUpdateRequest(Messages.StateUpdateRequest request) {
        tryToDeliver(request);
    }

    private void tryToDeliver(Messages.StateUpdateRequest stateUpdateRequest) {

        System.err.println("Check [" + stateUpdateRequest.getTimestamp() + "] lastApplied(" + lastApplied + ") and applyAfter(" + stateUpdateRequest.getApplyAfter() + ")");

        if(lastApplied >= stateUpdateRequest.getApplyAfter()) {

            System.err.println("delivery");
            deliver(createStateUpdateFromRequest(stateUpdateRequest));
            lastApplied = Math.max(lastApplied, stateUpdateRequest.getTimestamp());

            Set<StateUpdate> waitingUpdatesToRemove = new HashSet<>();

            for(StateUpdate waitingUpdate : waitingUpdates) {

                if(lastApplied >= waitingUpdate.getApplyAfter()) {
                    deliver(waitingUpdate);
                    lastApplied = Math.max(lastApplied, waitingUpdate.getTimestamp());
                    waitingUpdatesToRemove.add(waitingUpdate);
                } else {
                    break;
                }
            }

            waitingUpdates.removeAll(waitingUpdatesToRemove);
        } else {
            System.err.println("waitingUpdate: " + stateUpdateRequest.getTimestamp());
            waitingUpdates.add(createStateUpdateFromRequest(stateUpdateRequest));
            waitingUpdates.forEach(su -> System.err.println("waitingUpdate tm: " + su.getTimestamp()));
        }
    }

    private StateUpdate createStateUpdateFromRequest(Messages.StateUpdateRequest request) {
        return new StateUpdate(request.getTimestamp(),
                request.getApplyAfter(), new HashMap<>(request.getWritesMap()));
    }

    private void deliver(StateUpdate stateUpdate) {

        long transactionTimestamp = stateUpdate.getTimestamp();

        TransactionMetadata transaction = activeTransactions.get(transactionTimestamp);

        System.err.println("1.1");

        commitTransaction(stateUpdate);
        notifyAboutTransactionCommit(transactionTimestamp);

        System.err.println("1.2");

        Set<Long> awaitingForMe = transaction.getAwaitingForMe();

        for(Long waitingTransactionTimestamp : awaitingForMe) {

            TransactionMetadata waitingTransaction = activeTransactions.get(waitingTransactionTimestamp);

            waitingTransaction.getAwaitingToStart().remove(transactionTimestamp);

            if(waitingTransaction.getAwaitingToStart().isEmpty()) {

                BlockingQueue<ChannelHandlerContext> contextHolder = contexts.getOrNull(waitingTransactionTimestamp);

                if(contextHolder != null) {

                    ChannelHandlerContext context;

                    try {
                        context = contextHolder.take();
                        contextHolder.put(context);
                    } catch (InterruptedException e) {
                        throw new RuntimeException(e);
                    }

                    Messages.BeginTransactionResponse response = Messages.BeginTransactionResponse.newBuilder()
                            .setTimestamp(waitingTransactionTimestamp)
                            .setStartPossible(true)
                            .build();

                    Messages.Message message = Messages.Message.newBuilder().setBeginTransactionResponse(response).build();

                    context.writeAndFlush(message);

                    System.err.println(transactionTimestamp + " answered that " + waitingTransactionTimestamp + " can start");
                }
            }

            System.err.println(transactionTimestamp + ": " + waitingTransactionTimestamp + " is waiting for me");
        }

        System.err.println("2");

        System.err.println("3");

        activeTransactionFlags.remove(transactionTimestamp);
        activeTransactions.remove(transactionTimestamp);

        System.err.println("4");

        System.err.println("activeTransactions in update: " + activeTransactions.size());
        activeTransactions.forEach((aLong, transactionMetadata) -> {
            System.err.println("activeTransactions in update: " + aLong + " can start " + transactionMetadata.getAwaitingToStart().isEmpty());
        });
    }

    private void commitTransaction(StateUpdate stateUpdate) {

        long time = stateUpdate.getTimestamp();

        stateUpdate.getWrites().forEach((key, value) -> {

            if(value.isEmpty()) {
                resourceRepository.remove(key, time);
            } else {
                resourceRepository.put(key, value, time);
            }
        });

        liveTransactionsLock.lock();

        Multiset.Entry<Long> oldestTransaction = liveTransactions.firstEntry();

        if(oldestTransaction != null) {
            Long oldestTransactionTimestamp = oldestTransaction.getElement();
            resourceRepository.removeOutdatedVersions(oldestTransactionTimestamp);
        }

        liveTransactions.remove(stateUpdate.getTimestamp());
        liveTransactionsLock.unlock();
    }

    private void notifyAboutTransactionCommit(long transactionTimestamp) {

        BlockingQueue<ChannelHandlerContext> contextHolder = contexts.getOrNull(transactionTimestamp);

        System.err.println("Tried to notify about commit of " + transactionTimestamp);

        if(contextHolder != null) {
            ChannelHandlerContext context = contextHolder.peek();
            Messages.CommitResponse response = Messages.CommitResponse.newBuilder().build();
            Messages.Message message = Messages.Message.newBuilder().setCommitResponse(response).build();
            context.writeAndFlush(message);
            contexts.remove(transactionTimestamp);

            System.err.println("Notified about commit");
        }
    }

    @Override
    protected void updateToSnapshot(Object o) {
        /*Map.Entry<Long, Map<Long, TransactionMetadata>> snapshot = (Map.Entry<Long, Map<Long, TransactionMetadata>>) o;
        snapshot.getValue().entrySet()
                .forEach(entry -> activeTransactions.put(entry.getKey(),
                        new TransactionMetadata(entry.getValue().getReads(),
                                entry.getValue().getWrites(),
                                entry.getValue().getAwaitingForMe(),
                                entry.getValue().getAwaitingToStart(),
                                entry.getValue().getApplyAfter(),
                                entry.getValue().isFinished()
                ) ));
        timestamp = snapshot.getKey();
        System.err.println("updateToSnapshot");*/
    }

    @Override
    protected Object makeObjectSnapshot() {
        /*System.err.println("makeObjectSnapshot");
        return new AbstractMap.SimpleEntry<>(timestamp, activeTransactions);*/
        return 1L;
    }
}
