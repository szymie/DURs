package org.szymie;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.actor.Props;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import lsr.paxos.replica.Replica;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.szymie.server.strong.optimistic.FrontActor;
import org.szymie.server.strong.optimistic.ResourceRepository;
import org.szymie.server.strong.optimistic.SerializableCertificationService;
import org.szymie.server.strong.pessimistic.TransactionController;

import java.util.Properties;
import java.util.concurrent.atomic.AtomicLong;

@SpringBootApplication(exclude = TransactionController.class)
public class OptimisticReplica implements CommandLineRunner {

    @Value("${id}")
    protected int id;

    @Value("${address}")
    protected String address;

    @Value("${port}")
    protected int port;

    private SerializableCertificationService serializableCertificationService;

    @Bean
    public ActorSystem actorSystem() {

        Properties properties = new Properties();
        properties.setProperty("akka.remote.netty.tcp.hostname", address);
        properties.setProperty("akka.remote.netty.tcp.port", String.valueOf(port));

        Config overrides = ConfigFactory.parseProperties(properties);
        Config config = overrides.withFallback(ConfigFactory.load());

        return ActorSystem.create(String.format("replica-%d", id), config);
    }

    @Bean
    public ResourceRepository resourceRepository() {
        return new ResourceRepository();
    }

    @Bean
    public ActorRef frontActor(ActorSystem actorSystem, ResourceRepository resourceRepository, AtomicLong timestamp) {
        return actorSystem.actorOf(Props.create(FrontActor.class, resourceRepository, timestamp), "front");
    }

    @Bean
    public AtomicLong timestamp() {
        return new AtomicLong(0);
    }

    @Autowired
    public void setSerializableCertificationService(SerializableCertificationService serializableCertificationService) {
        this.serializableCertificationService = serializableCertificationService;
    }

    @Override
    public void run(String... strings) throws Exception {
        Replica replica = new Replica(new lsr.common.Configuration("src/main/resources/paxos.properties"), id, serializableCertificationService);
        replica.start();
    }
}
