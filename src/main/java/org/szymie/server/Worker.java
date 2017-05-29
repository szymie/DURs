package org.szymie.server;

import akka.actor.AbstractActor;
import org.szymie.messages.ReadRequest;
import org.szymie.messages.ReadResponse;

import java.util.Optional;
import java.util.concurrent.atomic.AtomicLong;

public class Worker extends AbstractActor {

    private ResourceRepository valuesRepository;
    private AtomicLong timestamp;

    public Worker(ResourceRepository valuesRepository, AtomicLong timestamp) {
        this.valuesRepository = valuesRepository;
        this.timestamp = timestamp;
    }

    @Override
    public Receive createReceive() {
        return receiveBuilder()
                .match(ReadRequest.class, readRequest -> {

                    long transactionTimestamp;

                    if(readRequest.timestamp == Long.MAX_VALUE) {
                        transactionTimestamp = timestamp.get();
                    } else {
                        transactionTimestamp = readRequest.timestamp;
                    }

                    Optional<ValueWithTimestamp> valueOptional = valuesRepository.get(readRequest.key, transactionTimestamp);
                    ReadResponse response = valueOptional.map(valueWithTimestamp -> new ReadResponse(valueWithTimestamp.value, transactionTimestamp))
                            .orElse(new ReadResponse(null, transactionTimestamp));

                    sender().tell(response, self());
                })
                .build();
    }
}
