package org.szymie.server.strong.pessimistic;

import lsr.service.SerializableService;
import org.szymie.messages.BeginTransactionRequest;

import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

public class BeginTransactionService extends SerializableService {

    private Map<Long, TransactionMetadata> activeTransactions;
    private long timestamp;

    public BeginTransactionService() {
        activeTransactions = new ConcurrentHashMap<>();
        timestamp = 0;
    }

    @Override
    protected Object execute(Object o) {

        BeginTransactionRequest request = (BeginTransactionRequest) o;
        TransactionMetadata newTransaction = new TransactionMetadata(request.reads.keySet(), request.writes.keySet());

        for(Map.Entry<Long, TransactionMetadata> entry : activeTransactions.entrySet()) {

            TransactionMetadata transaction = entry.getValue();
            transaction.lockForRead();

            if(!transaction.isFinished() && !Collections.disjoint(request.reads.keySet(), transaction.getWrites())) {
                newTransaction.addToWaitsFor(entry.getKey());
            }

            transaction.unlock();
        }

        return null;
    }

    @Override
    protected void updateToSnapshot(Object o) {

    }

    @Override
    protected Object makeObjectSnapshot() {
        return null;
    }
}
