package org.szymie;

import lsr.common.Configuration;
import lsr.paxos.client.Client;
import lsr.paxos.replica.Replica;

import java.io.IOException;

public class ReplicaMain {

    public static void main(String[] args) throws IOException {

        if (args.length > 2) {
            System.exit(1);
        }

        int replicaId = Integer.parseInt(args[0]);

        Replica replica = new Replica(new Configuration("src/main/resources/paxos.properties"), replicaId, new SimplifiedTotalOrderService());
        replica.start();

        System.out.println("replicaId: " + replicaId + ", started");

        System.in.read();
        System.exit(0);
    }
}
