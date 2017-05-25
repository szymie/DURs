package org.szymie.server;

import akka.actor.AbstractActor;
import akka.actor.ActorRef;
import akka.actor.Props;
import akka.actor.Terminated;
import akka.routing.ActorRefRoutee;
import akka.routing.RoundRobinRoutingLogic;
import akka.routing.Routee;
import akka.routing.Router;
import org.szymie.Configuration;
import org.szymie.client.TransactionMetadata;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class FrontActor extends AbstractActor {

    private Router router;

    private ResourceRepository resourceRepository;

    public FrontActor(ResourceRepository resourceRepository) {

        this.resourceRepository = resourceRepository;

        Configuration configuration = new Configuration();
        Integer numberOfWorkers = Integer.parseInt(configuration.get("replica_workers"));

        List<Routee> workers = Stream.generate(() -> {
            ActorRef r = getContext().actorOf(Props.create(Worker.class, resourceRepository));
            getContext().watch(r);
            return new ActorRefRoutee(r);
        }).limit(numberOfWorkers).collect(Collectors.toList());

        router = new Router(new RoundRobinRoutingLogic(), workers);
    }

    @Override
    public Receive createReceive() {
        return receiveBuilder()
                .match(TransactionMetadata.class, message -> router.route(message, getSender()))
                .match(Terminated.class, message -> {
                    router = router.removeRoutee(message.actor());
                    ActorRef r = getContext().actorOf(Props.create(Worker.class, resourceRepository));
                    getContext().watch(r);
                    router = router.addRoutee(new ActorRefRoutee(r));
                }).build();
    }
}
