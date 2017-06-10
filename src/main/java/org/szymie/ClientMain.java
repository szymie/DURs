package org.szymie;

import akka.actor.ActorSystem;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import org.szymie.client.SerializableTransaction;
import org.szymie.client.Transaction;
import org.szymie.client.TransactionFactory;

import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

public class ClientMain {

    public static void main(String[] args) throws InterruptedException {

        Properties properties = new Properties();
        properties.setProperty("akka.remote.netty.tcp.port", args[0]);

        Config overrides = ConfigFactory.parseProperties(properties);
        Config config = overrides.withFallback(ConfigFactory.load());

        ActorSystem actorSystem = ActorSystem.create("client", config);

        TransactionFactory transactionFactory = new TransactionFactory(actorSystem);

        //int numberOfReads = Integer.valueOf(args[1]);
        //int numberOfWrites = Integer.valueOf(args[2]);
        //int numberOfThreads = Integer.valueOf(args[3]);

        Benchmark benchmark = new Benchmark(transactionFactory, 300, 100, 8, 2, 0);

        benchmark.execute(Benchmark.SaturationLevel.LOW, 8);

        //long startTime = System.nanoTime();

        //long estimatedTime = System.nanoTime() - startTime;

        //System.out.println("Estimated time: " + TimeUnit.NANOSECONDS.toMillis(estimatedTime));
    }


}
