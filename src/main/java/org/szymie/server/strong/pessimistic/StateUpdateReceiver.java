package org.szymie.server.strong.pessimistic;

import io.netty.channel.ChannelHandlerContext;
import org.jgroups.Message;
import org.jgroups.ReceiverAdapter;
import org.jgroups.View;
import org.springframework.messaging.simp.SimpMessageSendingOperations;
import org.szymie.messages.CommitResponse;
import org.szymie.messages.Messages;
import org.szymie.messages.StateUpdate;
import org.szymie.server.strong.optimistic.ResourceRepository;

import java.util.*;
import java.util.stream.Collectors;

public class StateUpdateReceiver extends ReceiverAdapter implements HeadersCreator {

    private Map<Long, TransactionMetadata> activeTransactions;
    private ResourceRepository resourceRepository;
    private Map<Long, ChannelHandlerContext> contexts;
    private long lastApplied;
    private SortedSet<StateUpdate> waitingUpdates;

    public StateUpdateReceiver(Map<Long, TransactionMetadata> activeTransactions, ResourceRepository resourceRepository,
                               Map<Long, ChannelHandlerContext> contexts) {
        this.activeTransactions = activeTransactions;
        this.resourceRepository = resourceRepository;
        this.contexts = contexts;
        lastApplied = 0;
        waitingUpdates = new TreeSet<>();
    }

    //multiple deliveries
    @Override
    public void receive(Message message) {

        try {
            Thread.sleep(500);
        } catch (InterruptedException e) {
        }

        System.err.println("message:" + message.getObject());

        super.receive(message);
        tryToDeliver(message.getObject());
    }

    private void tryToDeliver(StateUpdate stateUpdate) {

        System.err.println("Check lastApplied(" + lastApplied + ") and applyAfter(" + stateUpdate.getApplyAfter() + ")");

        if(lastApplied >= stateUpdate.getApplyAfter()) {

            System.err.println("delivery");
            deliver(stateUpdate);
            lastApplied = stateUpdate.getTimestamp();

            Set<StateUpdate> waitingUpdatesToRemove = new HashSet<>();

            for(StateUpdate waitingUpdate : waitingUpdates) {

                if(lastApplied >= waitingUpdate.getApplyAfter()) {
                    deliver(waitingUpdate);
                    lastApplied = waitingUpdate.getTimestamp();
                    waitingUpdatesToRemove.add(waitingUpdate);
                } else {
                    break;
                }
            }

            waitingUpdates.removeAll(waitingUpdatesToRemove);
        } else {
            System.err.println("waitingUpdate: " + stateUpdate.getTimestamp());
            waitingUpdates.add(stateUpdate);
        }
    }

    private void deliver(StateUpdate stateUpdate) {

        long transactionTimestamp = stateUpdate.getTimestamp();
        TransactionMetadata transaction = activeTransactions.get(transactionTimestamp);

        System.err.println("1");

        transaction.acquireForWrite();

        System.err.println("1.1");

        commitTransaction(stateUpdate);
        notifyAboutTransactionCommit(transactionTimestamp);

        System.err.println("1.2");

        Set<Long> awaitingForMe = transaction.getAwaitingForMe();

        for(Long waitingTransactionTimestamp : awaitingForMe) {

            TransactionMetadata waitingTransaction = activeTransactions.get(waitingTransactionTimestamp);

            waitingTransaction.getAwaitingToStart().remove(transactionTimestamp);

            if(waitingTransaction.getAwaitingToStart().isEmpty()) {

                ChannelHandlerContext context = contexts.get(waitingTransactionTimestamp);

                if(context != null) {

                    Messages.BeginTransactionResponse response = Messages.BeginTransactionResponse.newBuilder()
                            .setTimestamp(waitingTransactionTimestamp)
                            .setStartPossible(true)
                            .build();

                    Messages.Message message = Messages.Message.newBuilder().setBeginTransactionResponse(response).build();

                    context.writeAndFlush(message);
                }
            }

            System.err.println(transactionTimestamp + ": " + waitingTransactionTimestamp + " is waiting for me");
        }

        System.err.println("2");

        transaction.finish();
        transaction.releaseWriteLock();

        System.err.println("3");

        activeTransactions.remove(transactionTimestamp);

        System.err.println("4");

        System.err.println("activeTransactions in update: " + activeTransactions.size());
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

        ChannelHandlerContext context = contexts.get(transactionTimestamp);

        if(context != null) {
            Messages.CommitResponse response = Messages.CommitResponse.newBuilder().build();
            Messages.Message message = Messages.Message.newBuilder().setCommitResponse(response).build();
            context.writeAndFlush(message);
            contexts.remove(transactionTimestamp);
        }
    }

    @Override
    public void viewAccepted(View view) {
        super.viewAccepted(view);
        String members = String.join(", ", view.getMembers().stream().map(Object::toString).collect(Collectors.toList()));
        System.out.println("View update: " + members);
    }
}
