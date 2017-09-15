package org.szymie.server.strong;

import lsr.common.Configuration;
import lsr.common.PID;
import lsr.paxos.client.SerializableClient;
import org.szymie.PaxosProcessesCreator;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

public class JPaxosLocalClientPool implements PaxosProcessesCreator {

    private ConcurrentHashMap<Long, SerializableClient> clients = new ConcurrentHashMap<>();
    private AtomicLong index = new AtomicLong(0);
    private List<PID> processes;

    private SerializableClient get(int numberOfClients) {

        long i = index.getAndIncrement() % numberOfClients;

        return clients.computeIfAbsent(i, ignore -> {

            SerializableClient client;
            try {
                client = new SerializableClient(new Configuration(processes));
            } catch (IOException e) {
                e.printStackTrace();
                throw new RuntimeException(e);
            }

            client.connect();

            return client;
        });
    }

    private JPaxosLocalClientPool(String paxosProcesses) {
        processes = createPaxosProcesses(paxosProcesses);
    }

    private static volatile JPaxosLocalClientPool jPaxosClientPool;

    public static SerializableClient get(String paxosProcesses, int numberOfClients) {

        if(jPaxosClientPool != null)
            return jPaxosClientPool.get(numberOfClients);

        synchronized(JPaxosClientPool.class) {
            if(jPaxosClientPool == null) {
                jPaxosClientPool = new JPaxosLocalClientPool(paxosProcesses);
            }
        }

        return jPaxosClientPool.get(numberOfClients);
    }
}
