package org.szymie.client.strong.pessimistic;


import org.szymie.client.strong.ReadWriteRemoveCommitTransaction;

import java.util.Map;

public interface Transaction extends ReadWriteRemoveCommitTransaction {
    void begin(Map<String, Integer> reads, Map<String, Integer> writes);
}