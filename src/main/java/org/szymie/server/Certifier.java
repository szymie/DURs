package org.szymie.server;

import akka.actor.AbstractActor;
import lsr.paxos.client.SerializableClient;
import org.szymie.messages.CertificationRequest;
import org.szymie.messages.CertificationResponse;

import java.io.IOException;

public class Certifier extends AbstractActor {

    private SerializableClient client;

    public Certifier() {
        try {
            client = new SerializableClient(new lsr.common.Configuration("src/main/resources/paxos.properties"));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
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
