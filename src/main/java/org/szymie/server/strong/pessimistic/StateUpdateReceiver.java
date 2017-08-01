package org.szymie.server.strong.pessimistic;

import org.jgroups.Message;
import org.jgroups.ReceiverAdapter;
import org.jgroups.View;
import org.springframework.messaging.simp.SimpMessageSendingOperations;
import org.szymie.messages.BeginTransactionResponse;
import org.szymie.messages.CommitResponse;
import org.szymie.messages.StateUpdate;
import org.szymie.server.strong.optimistic.ResourceRepository;

import java.util.*;
import java.util.stream.Collectors;

public class StateUpdateReceiver extends ReceiverAdapter implements HeadersCreator {

    private Map<Long, TransactionMetadata> activeTransactions;
    private ResourceRepository resourceRepository;
    private Map<Long, String> sessionIds;
    private SimpMessageSendingOperations messagingTemplate;
    private long lastApplied;
    private SortedSet<StateUpdate> waitingUpdates;

    public StateUpdateReceiver(Map<Long, TransactionMetadata> activeTransactions, ResourceRepository resourceRepository,
                               Map<Long, String> sessionIds, SimpMessageSendingOperations messagingTemplate) {
        this.activeTransactions = activeTransactions;
        this.resourceRepository = resourceRepository;
        this.sessionIds = sessionIds;
        this.messagingTemplate = messagingTemplate;
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

        applyChanges(stateUpdate);
        notifyAboutAppliedChanges(transactionTimestamp);

        System.err.println("1.2");

        Set<Long> awaitingForMe = transaction.getAwaitingForMe();

        for(Long waitingTransactionTimestamp : awaitingForMe) {

            TransactionMetadata waitingTransaction = activeTransactions.get(waitingTransactionTimestamp);

            waitingTransaction.getAwaitingToStart().remove(transactionTimestamp);

            if(waitingTransaction.getAwaitingToStart().isEmpty()) {

                String sessionId = sessionIds.get(waitingTransactionTimestamp);

                if(sessionId != null) {
                    messagingTemplate.convertAndSendToUser(sessionId, "/queue/begin-transaction-response",
                            new BeginTransactionResponse(waitingTransactionTimestamp, true), createHeaders(sessionId));
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

    private void applyChanges(StateUpdate stateUpdate) {

        long time = stateUpdate.getTimestamp();

        stateUpdate.getWrites().forEach((key, value) -> {

            if(value.isEmpty()) {
                resourceRepository.remove(key, time);
            } else {
                resourceRepository.put(key, value, time);
            }
        });
    }

    private void notifyAboutAppliedChanges(long transactionTimestamp) {

        String sessionId = sessionIds.get(transactionTimestamp);

        System.err.println("transactionTimestamp: " + transactionTimestamp + " sessionId: " + sessionId);

        if(sessionId != null) {
            messagingTemplate.convertAndSendToUser(sessionId, "/queue/commit-transaction-response",
                    new CommitResponse(true), createHeaders(sessionId));

            sessionIds.remove(transactionTimestamp);
        }
    }

    @Override
    public void viewAccepted(View view) {
        super.viewAccepted(view);
        String members = String.join(", ", view.getMembers().stream().map(Object::toString).collect(Collectors.toList()));
        System.out.println("View update: " + members);
    }
}
