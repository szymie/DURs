package org.szymie;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.actor.Props;
import lsr.paxos.replica.Replica;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Scope;
import org.szymie.server.CertificationService;
import org.szymie.server.FrontActor;
import org.szymie.server.ResourceRepository;
@SpringBootApplication
public class ReplicaMain implements CommandLineRunner {

    @Value("${id}")
    private int id;

    private CertificationService certificationService;

    @Autowired
    public ReplicaMain(CertificationService certificationService) {
        this.certificationService = certificationService;
    }

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
        return ActorSystem.create(String.format("replica-%d", id));
    }

    @Bean
    public ResourceRepository resourceRepository() {
        return new ResourceRepository();
    }

    @Bean
    public ActorRef frontActor(ActorSystem actorSystem, ResourceRepository resourceRepository) {
        return actorSystem.actorOf(Props.create(FrontActor.class, resourceRepository));
    }

    @Bean
    public CertificationService certificationService(ResourceRepository resourceRepository) {
        return new CertificationService(resourceRepository);
    }
}
