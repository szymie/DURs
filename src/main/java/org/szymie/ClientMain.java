package org.szymie;

import akka.actor.ActorSystem;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import org.szymie.client.SerializableTransaction;

import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

public class ClientMain {

    private class Operations {
        public static final int READ = 1;
        public static final int WRITE = 2;
    }

    public static void main(String[] args) throws InterruptedException {

        Properties properties = new Properties();
        properties.setProperty("akka.remote.netty.tcp.port", args[0]);

        Config overrides = ConfigFactory.parseProperties(properties);
        Config config = overrides.withFallback(ConfigFactory.load());

        ActorSystem actorSystem = ActorSystem.create("client", config);

        List<String> keys = IntStream.rangeClosed('a', 'z')
                .mapToObj(value -> String.valueOf((char) value))
                .collect(Collectors.toList());
        int keysSize = keys.size();

        Random randomKeys = new Random(8);
        Random randomValues = new Random(5);

        int numberOfReads = Integer.valueOf(args[1]);
        int numberOfWrites = Integer.valueOf(args[2]);
        int numberOfThreads = Integer.valueOf(args[3]);

        List<Thread> threads = IntStream.range(0, numberOfThreads)
                .mapToObj(ignore -> {

                        Map<String, Integer> operations = new HashMap<>();

                        for(int i = 0; i < numberOfReads;) {
                            String key = keys.get(randomKeys.nextInt(keysSize));
                            if(operations.put(key, Operations.READ) == null) {
                                i++;
                            }
                        }

                        for(int i = 0; i < numberOfWrites;) {
                            String key = keys.get(randomKeys.nextInt(keysSize));
                            if(operations.put(key, operations.getOrDefault(key, 0) | Operations.WRITE) == null) {
                                i++;
                            }
                        }

                        return operations;
                }).map(operations ->

                    new Thread(() -> {

                        while(true) {

                            SerializableTransaction transaction = new SerializableTransaction(actorSystem);

                            boolean commit;

                            do {
                                transaction.begin();

                                operations.forEach((key, operation) -> {

                                   if((operation & Operations.READ) != 0) {
                                       transaction.read(key);
                                   }

                                   if((operation & Operations.WRITE) != 0) {
                                       transaction.write(key, String.valueOf(randomValues.nextInt()));
                                   }
                                });

                                commit = transaction.commit();

                                System.err.println("commit: " + commit);

                            } while(!commit);
                        }
                    })
                ).collect(Collectors.toList());

        long startTime = System.nanoTime();

        threads.forEach(Thread::start);
        threads.forEach((thread) -> {
            try {
                thread.join();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        });

        long estimatedTime = System.nanoTime() - startTime;

        System.out.println("Estimated time: " + TimeUnit.NANOSECONDS.toMillis(estimatedTime));
    }
}
