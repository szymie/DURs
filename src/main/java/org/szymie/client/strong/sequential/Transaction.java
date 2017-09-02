package org.szymie.client.strong.sequential;

import org.szymie.client.strong.ReadWriteRemoveCommitTransaction;

public interface Transaction extends ReadWriteRemoveCommitTransaction {
    void begin();
}
