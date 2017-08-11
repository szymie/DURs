package org.szymie.client.strong.optimistic;

import org.szymie.client.strong.ReadWriteRemoveCommitTransaction;

public interface Transaction extends ReadWriteRemoveCommitTransaction {
    void begin();
}
