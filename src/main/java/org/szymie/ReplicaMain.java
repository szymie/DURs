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
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.szymie.server.CertificationService;
import org.szymie.server.FrontActor;
import org.szymie.server.ResourceRepository;
import org.szymie.server.Worker;

import java.util.Properties;
import java.util.concurrent.atomic.AtomicLong;

@SpringBootApplication
public class ReplicaMain implements CommandLineRunner {

    @Value("${id}")
    private int id;

    @Value("${address}")
    private String address;

    @Value("${port}")
    private int port;

    private CertificationService certificationService;

    public static void main(String[] args) {
        SpringApplication application = new SpringApplication(ReplicaMain.class);
        application.run(args);
    }

    @Override
    public void run(String... strings) throws Exception {
        Replica replica = new Replica(new lsr.common.Configuration("src/main/resources/paxos.properties"), id, certificationService);
        replica.start();
    }

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
        ActorRef actorRef = actorSystem.actorOf(Props.create(FrontActor.class, resourceRepository, timestamp), "front");
        return actorRef;
    }

    @Bean
    public AtomicLong timestamp() {
        return new AtomicLong(0);
    }

    @Autowired
    public void setCertificationService(CertificationService certificationService) {
        this.certificationService = certificationService;
    }
}
