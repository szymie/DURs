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

    private long lastApplied;
    private Set<StateUpdate> waitingUpdates;

    private ResourceRepository resourceRepository;
    private AtomicLong lastCommitted;

    private TreeMultiset<Long> liveTransactions;
    private Lock liveTransactionsLock;

    public TransactionService(int id, AtomicLong timestamp, Map<Long, TransactionMetadata> activeTransactions,
                              BlockingMap<Long, Boolean> activeTransactionFlags,
                              BlockingMap<Long, BlockingQueue<ChannelHandlerContext>> contexts, ResourceRepository resourceRepository, AtomicLong lastCommitted,
                              TreeMultiset<Long> liveTransactions, Lock liveTransactionsLock) {
        this.id = id;
        this.timestamp = timestamp;
        this.activeTransactions = activeTransactions;
        this.activeTransactionFlags = activeTransactionFlags;
        this.contexts = contexts;

        this.resourceRepository = resourceRepository;
        this.lastCommitted = lastCommitted;
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
                return handleStateUpdateRequest(message.getStateUpdateRequest());
            default:
                return new Object();
        }
    }


    private Messages.BeginTransactionResponse handleBeginTransaction(Messages.BeginTransactionRequest beginTransactionRequest) {

        request = beginTransactionRequest;

        long newTransactionTimestamp = timestamp.incrementAndGet();

        TransactionMetadata newTransaction = new TransactionMetadata(id, newTransactionTimestamp, beginTransactionRequest.getReadsMap().keySet(), beginTransactionRequest.getWritesMap().keySet());

        //System.err.println("activeTransactions: " + activeTransactions.size());

        if(id == beginTransactionRequest.getId()) {
            //System.err.println("set context for at: " + id + " where timestamp: " + newTransactionTimestamp);
            contexts.put(newTransactionTimestamp, new ArrayBlockingQueue<>(1));
        }

        boolean startPossible = true;

        for(Map.Entry<Long, TransactionMetadata> entry : activeTransactions.entrySet()) {

            TransactionMetadata transaction = entry.getValue();

            if(isAwaitingToStartNeeded(transaction)) {
                newTransaction.getAwaitingToStart().add(entry.getKey());
                transaction.getAwaitingForMe().add(newTransaction);
                startPossible = false;
            }
        }

        activeTransactions.put(newTransactionTimestamp, newTransaction);
        activeTransactionFlags.put(newTransactionTimestamp, startPossible);

        //System.err.println(newTransactionTimestamp + " can start " + startPossible);
        //newTransaction.getAwaitingToStart().forEach(transactionId -> System.err.println(newTransactionTimestamp + " is waiting for " + transactionId));

        return Messages.BeginTransactionResponse.newBuilder()
                .setStartPossible(startPossible)
                .setTimestamp(newTransactionTimestamp)
                .build();
    }

    private boolean isAwaitingToStartNeeded(TransactionMetadata transaction) {
        return !Collections.disjoint(request.getReadsMap().keySet(), transaction.getWrites());
    }

    private Messages.StateUpdateResponse handleStateUpdateRequest(Messages.StateUpdateRequest stateUpdateRequest) {
        tryToDeliver(stateUpdateRequest);
        return Messages.StateUpdateResponse.newBuilder().build();
    }

    private void tryToDeliver(Messages.StateUpdateRequest stateUpdateRequest) {

        if(lastApplied + 1 == stateUpdateRequest.getTimestamp()) {

            deliver(createStateUpdateFromRequest(stateUpdateRequest));
            lastApplied = Math.max(lastApplied, stateUpdateRequest.getTimestamp());

            Set<StateUpdate> waitingUpdatesToRemove = new HashSet<>();

            for(StateUpdate waitingUpdate : waitingUpdates) {

                if(lastApplied + 1 == waitingUpdate.getTimestamp()) {
                    deliver(waitingUpdate);
                    lastApplied = Math.max(lastApplied, waitingUpdate.getTimestamp());
                    waitingUpdatesToRemove.add(waitingUpdate);
                } else {
                    break;
                }
            }

            waitingUpdates.removeAll(waitingUpdatesToRemove);
        } else {
            waitingUpdates.add(createStateUpdateFromRequest(stateUpdateRequest));
        }
    }

    private StateUpdate createStateUpdateFromRequest(Messages.StateUpdateRequest request) {
        return new StateUpdate(request.getTimestamp(),
                request.getApplyAfter(), new HashMap<>(request.getWritesMap()));
    }

    private void deliver(StateUpdate stateUpdate) {

        long transactionTimestamp = stateUpdate.getTimestamp();

        TransactionMetadata transaction = activeTransactions.get(transactionTimestamp);

        commitTransaction(stateUpdate);
        notifyAboutTransactionCommit(transactionTimestamp);

        Set<TransactionMetadata> awaitingForMe = transaction.getAwaitingForMe();

        for(TransactionMetadata waitingTransaction : awaitingForMe) {

            waitingTransaction.getAwaitingToStart().remove(transactionTimestamp);

            if(waitingTransaction.getAwaitingToStart().isEmpty()) {

                BlockingQueue<ChannelHandlerContext> contextHolder = contexts.getOrNull(waitingTransaction.getTimestamp());

                if(contextHolder != null) {

                    ChannelHandlerContext context;

                    try {
                        context = contextHolder.take();
                        contextHolder.put(context);
                    } catch (InterruptedException e) {
                        throw new RuntimeException(e);
                    }

                    Messages.BeginTransactionResponse response = Messages.BeginTransactionResponse.newBuilder()
                            .setTimestamp(waitingTransaction.getTimestamp())
                            .setStartPossible(true)
                            .build();

                    Messages.Message message = Messages.Message.newBuilder().setBeginTransactionResponse(response).build();

                    context.writeAndFlush(message);
                }
            }
        }

        activeTransactionFlags.remove(transactionTimestamp);
        activeTransactions.remove(transactionTimestamp);
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

        lastCommitted.getAndAccumulate(time, Math::max);

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

        if(contextHolder != null) {
            ChannelHandlerContext context = contextHolder.peek();
            Messages.CommitResponse response = Messages.CommitResponse.newBuilder().build();
            Messages.Message message = Messages.Message.newBuilder().setCommitResponse(response).build();
            context.writeAndFlush(message);
            contexts.remove(transactionTimestamp);
        }
    }

    @Override
    protected void updateToSnapshot(Object o) {
    }

    @Override
    protected Object makeObjectSnapshot() {
        return 1L;
    }
}
