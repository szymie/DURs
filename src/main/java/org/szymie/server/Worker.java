package org.szymie.server;

import akka.actor.AbstractActor;
import org.szymie.messages.ReadRequest;
import org.szymie.messages.ReadResponse;

import java.util.Optional;

public class Worker extends AbstractActor {

    private ResourceRepository valuesRepository;

    public Worker(ResourceRepository valuesRepository) {
        this.valuesRepository = valuesRepository;
    }

    @Override
    public Receive createReceive() {
        return receiveBuilder()
                .match(ReadRequest.class, readRequest -> {

                    Optional<Value> valueOptional = valuesRepository.get(readRequest.key, readRequest.timestamp);
                    ReadResponse response = valueOptional.map(value -> new ReadResponse(value.value, value.timestamp, false))
                            .orElse(new ReadResponse(null, Integer.MAX_VALUE, true));

                    sender().tell(response, self());
                })
                .build();
    }
}
