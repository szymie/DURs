package org.szymie;

import lsr.common.PID;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.StringTokenizer;
import java.util.stream.Collectors;

public interface PaxosProcessesCreator {

    default List<PID> createPaxosProcesses(String paxosProcesses) {

        if(!paxosProcesses.isEmpty()) {

            return Arrays.stream(paxosProcesses.split(",")).map(process -> {

                StringTokenizer tokenizer = new StringTokenizer(process, ":");

                int id = Integer.parseInt(tokenizer.nextToken());
                String hostname = tokenizer.nextToken();
                int replicaPort = Integer.parseInt(tokenizer.nextToken());
                int clientPort = Integer.parseInt(tokenizer.nextToken());

                return new PID(id, hostname, replicaPort, clientPort);
            }).collect(Collectors.toList());
        } else {
            return Collections.emptyList();
        }
    }
}
