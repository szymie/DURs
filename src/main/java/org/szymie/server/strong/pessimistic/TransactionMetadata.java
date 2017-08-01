package org.szymie.server.strong.pessimistic;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class TransactionMetadata {

    private Set<String> reads;
    private Set<String> writes;
    private Set<Long> awaitingForMe;
    private Set<Long> awaitingToStart;
    private long applyAfter;
    private boolean finished;
    private ReadWriteLock lock;

    public TransactionMetadata(Set<String> reads, Set<String> writes) {

        this.reads = reads;
        this.writes = writes;

        awaitingForMe = new HashSet<>();
        awaitingToStart = Collections.newSetFromMap(new ConcurrentSkipListMap<>());

        applyAfter = 0;
        finished = false;

        lock = new ReentrantReadWriteLock();
    }

    public TransactionMetadata(Set<String> reads, Set<String> writes, Set<Long> awaitingForMe, Set<Long> awaitingToStart, long applyAfter, boolean finished) {
        this(reads, writes);
        this.reads = reads;
        this.writes = writes;
        this.awaitingForMe = awaitingForMe;
        this.awaitingToStart = awaitingToStart;
        this.applyAfter = applyAfter;
        this.finished = finished;
    }

    public void acquireReadLock() {
        lock.readLock().lock();
    }

    public void acquireForWrite() {
        lock.writeLock().lock();
    }

    public void releaseReadLock() {
        lock.readLock().unlock();
    }

    public void releaseWriteLock() {
        lock.writeLock().unlock();
    }

    public void setApplyAfter(long applyAfter) {
        this.applyAfter = applyAfter;
    }

    public long getApplyAfter() {
        return applyAfter;
    }

    public boolean isFinished() {
        return finished;
    }

    public Set<Long> getAwaitingToStart() {
        return awaitingToStart;
    }

    public Set<Long> getAwaitingForMe() {
        return awaitingForMe;
    }

    public void finish() {
        finished = true;
    }

    public Set<String> getReads() {
        return reads;
    }

    public Set<String> getWrites() {
        return writes;
    }
}
