package org.szymie.client;

import akka.actor.ActorSystem;

public class TransactionFactory {

    private ActorSystem actorSystem;

    public TransactionFactory(ActorSystem actorSystem) {
        this.actorSystem = actorSystem;
    }

    public Transaction newSerializableTransaction() {
        return new SerializableTransaction(actorSystem);
    }
}
