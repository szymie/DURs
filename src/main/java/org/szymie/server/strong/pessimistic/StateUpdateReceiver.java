package org.szymie.server.strong.pessimistic;

import com.google.common.collect.Multiset;
import com.google.common.collect.TreeMultiset;
import io.netty.channel.ChannelHandlerContext;
import org.jgroups.Message;
import org.jgroups.ReceiverAdapter;
import org.jgroups.View;
import org.springframework.messaging.simp.SimpMessageSendingOperations;
import org.szymie.BlockingMap;
import org.szymie.messages.CommitResponse;
import org.szymie.messages.Messages;
import org.szymie.messages.StateUpdate;
import org.szymie.server.strong.optimistic.ResourceRepository;

import java.util.*;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Lock;
import java.util.stream.Collectors;

public class StateUpdateReceiver extends ReceiverAdapter {

    private Map<Long, TransactionMetadata> activeTransactions;
    private BlockingMap<Long, Boolean> activeTransactionFlags;
    private ResourceRepository resourceRepository;
    private BlockingMap<Long, BlockingQueue<ChannelHandlerContext>> contexts;
    private TreeMultiset<Long> liveTransactions;
    private Lock liveTransactionsLock;
    private AtomicLong lastCommitted;
    private long lastApplied;
    private SortedSet<StateUpdate> waitingUpdates;
    
    //private List<Long> delivered = new LinkedList<>();

    public StateUpdateReceiver(Map<Long, TransactionMetadata> activeTransactions, BlockingMap<Long, Boolean> activeTransactionFlags,
                               ResourceRepository resourceRepository, BlockingMap<Long, BlockingQueue<ChannelHandlerContext>> contexts,
                               TreeMultiset<Long> liveTransactions, Lock liveTransactionsLock,
                               AtomicLong lastCommitted) {
        this.activeTransactions = activeTransactions;
        this.activeTransactionFlags = activeTransactionFlags;
        this.resourceRepository = resourceRepository;
        this.contexts = contexts;
        this.liveTransactions = liveTransactions;
        this.liveTransactionsLock = liveTransactionsLock;
        this.lastCommitted = lastCommitted;
        lastApplied = 0;
        waitingUpdates = new TreeSet<>();


    }

    @Override
    public synchronized void receive(Message message) {

        StateUpdate stateUpdate = message.getObject();
        //delivered.add(stateUpdate.getTimestamp());

        System.err.println("message:" + message.getObject() + " timestamp: " + stateUpdate.getTimestamp());

        System.err.flush();

        tryToDeliver(message.getObject());

        /*System.err.println(delivered.stream().sorted()
                .map(String::valueOf)
                .collect(Collectors.joining(", ")));*/
    }

    private void tryToDeliver(StateUpdate stateUpdate) {

        System.err.println("Check [" + stateUpdate.getTimestamp() + "] lastApplied(" + lastApplied + ") and applyAfter(" + stateUpdate.getApplyAfter() + ")");

        if(lastApplied >= stateUpdate.getApplyAfter()) {

            System.err.println("delivery");
            deliver(stateUpdate);
            lastApplied = Math.max(lastApplied, stateUpdate.getTimestamp());

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
            System.err.println("waitingUpdate: " + stateUpdate.getTimestamp());
            waitingUpdates.add(stateUpdate);
            waitingUpdates.forEach(su -> System.err.println("waitingUpdate tm: " + su.getTimestamp()));
        }

        System.err.println("tryToDeliver");
    }

    private void deliver(StateUpdate stateUpdate) {

        long transactionTimestamp = stateUpdate.getTimestamp();

        activeTransactionFlags.get(transactionTimestamp);
        TransactionMetadata transaction = activeTransactions.get(transactionTimestamp);

        System.err.println("1");

        transaction.acquireForWrite();

        System.err.println("1.1");

        commitTransaction(stateUpdate);
        notifyAboutTransactionCommit(transactionTimestamp);

        System.err.println("1.2");

        Set<TransactionMetadata> awaitingForMe = transaction.getAwaitingForMe();

        for(TransactionMetadata waitingTransaction : awaitingForMe) {

            if(!waitingTransaction.isFinished()) {

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

                        System.err.println(transactionTimestamp + " answered that " + waitingTransaction.getTimestamp() + " can start");
                    }
                }

            }

            System.err.println(transactionTimestamp + ": " + waitingTransaction.getTimestamp() + " is waiting for me");
        }

        System.err.println("2");

        transaction.finish();
        transaction.releaseWriteLock();

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

            System.err.println("Notified about commit of " + transactionTimestamp);
        }
    }

    @Override
    public void viewAccepted(View view) {
        super.viewAccepted(view);
        String members = String.join(", ", view.getMembers().stream().map(Object::toString).collect(Collectors.toList()));
        System.out.println("View update: " + members);
    }
}
