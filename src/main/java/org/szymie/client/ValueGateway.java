package org.szymie.client;

import akka.actor.ActorSelection;
import akka.actor.ActorSystem;
import akka.pattern.Patterns;
import org.szymie.Configuration;

import java.util.List;
import java.util.Random;
import java.util.concurrent.TimeUnit;

import org.szymie.messages.ReadRequest;
import org.szymie.messages.ReadResponse;
import scala.concurrent.Future;
import scala.concurrent.Await;
import akka.util.Timeout;
import scala.concurrent.duration.Duration;


public class ValueGateway {

    private ActorSystem actorSystem;
    private TransactionMetadata transactionMetadata;
    private Configuration configuration;
    private boolean sessionOpen;
    private int readTimeout;

    private Random random;
    private ActorSelection replicaActor;


    public ValueGateway(ActorSystem actorSystem) {

        this.actorSystem = actorSystem;
        transactionMetadata = new TransactionMetadata();
        configuration = new Configuration();
        sessionOpen = false;
        readTimeout = Integer.parseInt(configuration.get("read_timeout"));

        random = new Random(System.currentTimeMillis());
    }

    public void openSession() {
        String replicaEndpoint = getReplicaEndpoint();
        replicaActor = actorSystem.actorSelection("akka.tcp://replicaSystem@" + replicaEndpoint + "/user/front");
        //TODO: send first message to ensure the connection is established
        sessionOpen = true;
    }

    public String getReplicaEndpoint() {
        List<String> replicas = configuration.getAsList("replicas");
        return getRandomElement(replicas);
    }

    public String getRandomElement(List<String> list) {
        return list.get(random.nextInt(list.size()));
    }

    public void closeSession() {
        sessionOpen = false;
    }

    public boolean isSessionOpen() {
        return sessionOpen;
    }

    public String read(String key) {

        String value = transactionMetadata.writtenValues.get(key);

        if(value == null) {
            value = transactionMetadata.readValues.get(key);
        }

        if(value == null) {

            ReadRequest readRequest = new ReadRequest(key, transactionMetadata.timestamp);
            Timeout timeout = new Timeout(Duration.create(readTimeout, TimeUnit.SECONDS));

            ReadResponse readResponse = null;

            while(readResponse == null) {

                Future<Object> future = Patterns.ask(replicaActor, readRequest, timeout);

                try {
                    readResponse = (ReadResponse) Await.result(future, timeout.duration());
                } catch (Exception ignore) {
                    openSession();
                }
            }

            if(transactionMetadata.timestamp == Long.MAX_VALUE) {
                transactionMetadata.timestamp = readResponse.timestamp;
            }

            value = readResponse.value;
        }

        transactionMetadata.readValues.put(key, value);

        return value;
    }

    public void write(String key, String value) {
        transactionMetadata.writtenValues.put(key, value);
    }

    public void clear() {
        transactionMetadata.clear();
    }
}
