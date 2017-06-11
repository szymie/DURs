package org.szymie.server;

import akka.actor.AbstractActor;
import akka.actor.ActorRef;
import akka.actor.Props;
import akka.actor.Terminated;
import akka.event.Logging;
import akka.event.LoggingAdapter;
import akka.routing.ActorRefRoutee;
import akka.routing.RoundRobinRoutingLogic;
import akka.routing.Routee;
import akka.routing.Router;
import org.szymie.Configuration;
import org.szymie.messages.CertificationRequest;
import org.szymie.messages.ReadRequest;

import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class FrontActor extends AbstractActor {

    private Router router;

    private ResourceRepository resourceRepository;
    private AtomicLong timestamp;

    private LoggingAdapter logger = Logging.getLogger(getContext().getSystem(), this);

    public FrontActor(ResourceRepository resourceRepository, AtomicLong timestamp) {

        this.resourceRepository = resourceRepository;
        this.timestamp = timestamp;

        Configuration configuration = new Configuration();
        Integer numberOfWorkers = Integer.parseInt(configuration.get("replica_workers"));

        List<Routee> workers = Stream.generate(() -> {
            ActorRef r = getContext().actorOf(Props.create(Worker.class, resourceRepository, timestamp));
            getContext().watch(r);
            return new ActorRefRoutee(r);
        }).limit(numberOfWorkers).collect(Collectors.toList());

        router = new Router(new RoundRobinRoutingLogic(), workers);
    }

    @Override
    public Receive createReceive() {
        return receiveBuilder()
                .match(CertificationRequest.class, message -> router.route(message, getSender()))
                .match(ReadRequest.class, message -> {
                    logger.debug("FrontActor, ReadRequest");
                    router.route(message, getSender());
                })
                .match(Terminated.class, message -> {
                    router = router.removeRoutee(message.actor());
                    ActorRef r = getContext().actorOf(Props.create(Worker.class, resourceRepository, timestamp));
                    getContext().watch(r);
                    router = router.addRoutee(new ActorRefRoutee(r));
                })
                .build();
    }
}
