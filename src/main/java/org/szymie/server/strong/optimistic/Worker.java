package org.szymie.server.strong.optimistic;

import akka.actor.AbstractActor;
import akka.event.Logging;
import akka.event.LoggingAdapter;
import org.szymie.messages.ReadRequest;
import org.szymie.messages.ReadResponse;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicLong;

public class Worker extends AbstractActor {

    private ResourceRepository valuesRepository;
    private AtomicLong timestamp;

    private LoggingAdapter logger = Logging.getLogger(getContext().getSystem(), this);

    public Worker(ResourceRepository valuesRepository, AtomicLong timestamp) {
        this.valuesRepository = valuesRepository;
        this.timestamp = timestamp;
    }

    @Override
    public Receive createReceive() {
        return receiveBuilder()
                .match(ReadRequest.class, readRequest -> {

                    logger.debug("Worker, ReadRequest");

                    long transactionTimestamp;

                    if(readRequest.timestamp == Long.MAX_VALUE) {
                        transactionTimestamp = timestamp.get();
                    } else {
                        transactionTimestamp = readRequest.timestamp;
                    }

                    Optional<ValueWithTimestamp> valueOptional = valuesRepository.get(readRequest.key, transactionTimestamp);
                    ReadResponse response = valueOptional.map(valueWithTimestamp -> new ReadResponse(valueWithTimestamp.value, transactionTimestamp, valueWithTimestamp.fresh))
                            .orElse(new ReadResponse(null, transactionTimestamp, true));

                    sender().tell(response, self());
                })
                .build();
    }
}
