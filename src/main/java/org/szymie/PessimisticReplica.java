package org.szymie;


import lsr.paxos.replica.Replica;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.messaging.MessageHeaders;
import org.springframework.messaging.handler.annotation.MessageMapping;
import org.springframework.messaging.simp.SimpMessageHeaderAccessor;
import org.springframework.messaging.simp.SimpMessageSendingOperations;
import org.springframework.messaging.simp.SimpMessageType;
import org.springframework.messaging.simp.annotation.SendToUser;
import org.springframework.stereotype.Controller;
import org.szymie.messages.BeginTransactionResponse;
import org.szymie.server.strong.pessimistic.BeginTransactionService;

@SpringBootApplication
public class PessimisticReplica implements CommandLineRunner {

    @Value("${id}")
    protected int id;

    private BeginTransactionService beginTransactionService;

    @Autowired
    public void setBeginTransactionService(BeginTransactionService beginTransactionService) {
        this.beginTransactionService = beginTransactionService;
    }

    @Override
    public void run(String... strings) throws Exception {
        Replica replica = new Replica(new lsr.common.Configuration("src/main/resources/paxos.properties"), id, beginTransactionService);
        replica.start();
    }

}
