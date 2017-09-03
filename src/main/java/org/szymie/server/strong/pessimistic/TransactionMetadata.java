package org.szymie.server.strong.pessimistic;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class TransactionMetadata {

    private int replicaId;
    private long timestamp;
    private Set<String> reads;
    private Set<String> writes;
    private Set<TransactionMetadata> awaitingForMe;
    private Set<Long> awaitingToStart;
    private long applyAfter;
    private boolean finished;
    private ReadWriteLock lock;

    private boolean canStart;

    public TransactionMetadata(int replicaId, long timestamp, Set<String> reads, Set<String> writes) {

        this.replicaId = replicaId;
        this.timestamp = timestamp;
        this.reads = reads;
        this.writes = writes;

        awaitingForMe = new HashSet<>();
        awaitingToStart = Collections.newSetFromMap(new ConcurrentSkipListMap<>());

        applyAfter = 0;
        finished = false;

        lock = new ReentrantReadWriteLock();

        canStart = false;
    }

    public TransactionMetadata(int replicaId, long timestamp, Set<String> reads, Set<String> writes, Set<TransactionMetadata> awaitingForMe, Set<Long> awaitingToStart, long applyAfter, boolean finished) {
        this(replicaId, timestamp, reads, writes);
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

    public Set<TransactionMetadata> getAwaitingForMe() {
        return awaitingForMe;
    }

    public void finish() {
        finished = true;
    }

    public int getReplicaId() {
        return replicaId;
    }

    public void setReplicaId(int replicaId) {
        this.replicaId = replicaId;
    }


    public long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }

    public Set<String> getReads() {
        return reads;
    }

    public Set<String> getWrites() {
        return writes;
    }

    public boolean canStart() {
        return canStart;
    }

    public void setCanStart(boolean canStart) {
        this.canStart = canStart;
    }
}
