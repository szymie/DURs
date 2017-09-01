package org.szymie.server.strong.pessimistic;

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
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

public class StateUpdateReceiver extends ReceiverAdapter {

    private Map<Long, TransactionMetadata> activeTransactions;
    private BlockingMap<Long, Boolean> activeTransactionFlags;
    private ResourceRepository resourceRepository;
    private BlockingMap<Long, BlockingQueue<ChannelHandlerContext>> contexts;
    private final AtomicLong timestamp;
    private long lastApplied;
    private SortedSet<StateUpdate> waitingUpdates;

    public StateUpdateReceiver(Map<Long, TransactionMetadata> activeTransactions, BlockingMap<Long, Boolean> activeTransactionFlags,
                               ResourceRepository resourceRepository, BlockingMap<Long, BlockingQueue<ChannelHandlerContext>> contexts, AtomicLong timestamp) {
        this.activeTransactions = activeTransactions;
        this.activeTransactionFlags = activeTransactionFlags;
        this.resourceRepository = resourceRepository;
        this.contexts = contexts;
        this.timestamp = timestamp;
        lastApplied = 0;
        waitingUpdates = new TreeSet<>();
    }
    
    @Override
    public void receive(Message message) {

        System.err.println("message:" + message.getObject());

        super.receive(message);
        tryToDeliver(message.getObject());
    }

    private void tryToDeliver(StateUpdate stateUpdate) {

        System.err.println("Check [" + stateUpdate.getTimestamp() + "] lastApplied(" + lastApplied + ") and applyAfter(" + stateUpdate.getApplyAfter() + ")");

        if(lastApplied >= stateUpdate.getApplyAfter()) {

            System.err.println("delivery");
            deliver(stateUpdate);
            lastApplied = stateUpdate.getTimestamp();

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
    }

    private void deliver(StateUpdate stateUpdate) {

        long transactionTimestamp = stateUpdate.getTimestamp();

        waitForActiveTransaction(transactionTimestamp);

        activeTransactionFlags.get(transactionTimestamp);
        TransactionMetadata transaction = activeTransactions.get(transactionTimestamp);

        System.err.println("1");

        transaction.acquireForWrite();

        System.err.println("1.1");

        commitTransaction(stateUpdate);
        notifyAboutTransactionCommit(transactionTimestamp);

        System.err.println("1.2");

        Set<Long> awaitingForMe = transaction.getAwaitingForMe();

        for(Long waitingTransactionTimestamp : awaitingForMe) {

            activeTransactionFlags.get(waitingTransactionTimestamp);
            TransactionMetadata waitingTransaction = activeTransactions.get(waitingTransactionTimestamp);

            waitingTransaction.getAwaitingToStart().remove(transactionTimestamp);

            if(waitingTransaction.getAwaitingToStart().isEmpty()) {

                BlockingQueue<ChannelHandlerContext> contextHolder = contexts.get(waitingTransactionTimestamp);

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

    private void waitForActiveTransaction(long transactionTimestamp) {

        try {
            synchronized(timestamp) {
                while(timestamp.get() < transactionTimestamp) {
                    timestamp.wait();
                }
            }
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
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
    }

    private void notifyAboutTransactionCommit(long transactionTimestamp) {

        BlockingQueue<ChannelHandlerContext> contextHolder = contexts.get(transactionTimestamp);

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
    public void viewAccepted(View view) {
        super.viewAccepted(view);
        String members = String.join(", ", view.getMembers().stream().map(Object::toString).collect(Collectors.toList()));
        System.out.println("View update: " + members);
    }
}
