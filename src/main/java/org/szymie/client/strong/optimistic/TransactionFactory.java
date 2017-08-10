package org.szymie.client.strong.optimistic;

import akka.actor.ActorSystem;

public class TransactionFactory {

    public Transaction newSerializableTransaction() {
        return new NettySerializableTransaction();
    }
}
