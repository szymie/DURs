package org.szymie.server.strong;


import lsr.common.Configuration;
import lsr.common.PID;
import lsr.paxos.client.SerializableClient;
import org.szymie.PaxosProcessesCreator;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

public class JPaxosClientPool implements PaxosProcessesCreator {

    private int id;
    private ConcurrentHashMap<Integer, SerializableClient> clients = new ConcurrentHashMap<>();
    private AtomicInteger index = new AtomicInteger(0);
    private List<PID> processes;

    private SerializableClient get(int numberOfClients) {

        int i = index.getAndIncrement() % numberOfClients;

        return clients.computeIfAbsent(i, ignore -> {
            SerializableClient client = new SerializableClient(new Configuration(processes), id);
            client.connect();
            return client;
        });
    }

    private JPaxosClientPool(int id, String paxosProcesses) {
        this.id = id;
        processes = createPaxosProcesses(paxosProcesses);
    }

    private static volatile JPaxosClientPool jPaxosClientPool;

    public static SerializableClient get(int id, String paxosProcesses, int numberOfClients) {

        if(jPaxosClientPool != null)
            return jPaxosClientPool.get(numberOfClients);

        synchronized(JPaxosClientPool.class) {
            if(jPaxosClientPool == null) {
                jPaxosClientPool = new JPaxosClientPool(id, paxosProcesses);
            }
        }

        return jPaxosClientPool.get(numberOfClients);
    }
}
