package org.szymie.server.strong.pessimistic;

import java.util.Set;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class TransactionMetadata {

    private Set<String> reads;
    private Set<String> writes;

    private Set<Long> waitsFor;

    private boolean finished;
    private Lock readLock;
    private Lock writeLock;

    public TransactionMetadata(Set<String> reads, Set<String> writes) {

        this.reads = reads;
        this.writes = writes;

        finished = false;

        ReadWriteLock lock = new ReentrantReadWriteLock(true);
        readLock = lock.readLock();
        writeLock = lock.writeLock();
    }

    public void lockForRead() {
        readLock.lock();
    }

    public void unlock() {
        readLock.unlock();
    }

    public boolean isFinished() {
        return finished;
    }

    public void addToWaitsFor(Long transactionId) {
        waitsFor.add(transactionId);
    }

    public void finish() {
        writeLock.lock();
        finished = true;
        writeLock.unlock();
    }

    public Set<String> getReads() {
        return reads;
    }

    public Set<String> getWrites() {
        return writes;
    }
}
