package org.szymie.server;

import akka.actor.AbstractActor;
import lsr.paxos.client.SerializableClient;
import org.szymie.messages.CertificationRequest;
import org.szymie.messages.CertificationResponse;

public class Certifier extends AbstractActor {

    private SerializableClient client;

    public Certifier(SerializableClient client) {
        this.client = client;
    }

    @Override
    public Receive createReceive() {
        return receiveBuilder()
                .match(CertificationRequest.class, certificationRequest -> {
                    CertificationResponse response = (CertificationResponse) client.execute(certificationRequest);
                    sender().tell(response, self());
                }).build();
    }
}
