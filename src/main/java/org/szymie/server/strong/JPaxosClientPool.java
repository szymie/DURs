package org.szymie.server.strong;


import lsr.common.Configuration;
import lsr.common.PID;
import lsr.paxos.client.SerializableClient;
import org.szymie.PaxosProcessesCreator;

import java.io.IOException;
import java.util.List;

public class JPaxosClientPool implements PaxosProcessesCreator {

    private SerializableClient client;

    private JPaxosClientPool(int id, String paxosProcesses) {
        List<PID> processes = createPaxosProcesses(paxosProcesses);
        client = new SerializableClient(new Configuration(processes), id);
        client.connect();
    }

    private static volatile JPaxosClientPool jPaxosClientPool;

    public static SerializableClient get(int id, String paxosProcesses) {

        if(jPaxosClientPool != null)
            return jPaxosClientPool.client;

        synchronized(JPaxosClientPool.class) {
            if(jPaxosClientPool == null) {
                jPaxosClientPool = new JPaxosClientPool(id, paxosProcesses);
            }
        }

        return jPaxosClientPool.client;
    }
}
