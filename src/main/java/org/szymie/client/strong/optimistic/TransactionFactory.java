package org.szymie.client.strong.optimistic;

import akka.actor.ActorSystem;

public class TransactionFactory {

    private ActorSystem actorSystem;

    public TransactionFactory(ActorSystem actorSystem) {
        this.actorSystem = actorSystem;
    }

    public Transaction newSerializableTransaction() {
        return new WebSocketSerializableTransaction();
    }
}
