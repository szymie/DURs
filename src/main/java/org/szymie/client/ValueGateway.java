package org.szymie.client;

import akka.actor.ActorSelection;
import akka.actor.ActorSystem;
import akka.pattern.Patterns;
import org.szymie.Configuration;

import java.util.*;
import java.util.concurrent.TimeUnit;

import org.szymie.ValueWrapper;
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

        Map.Entry<Integer, String> replicaEndpoint = getReplicaEndpoint();

        System.err.println(replicaEndpoint.getKey() + " " + replicaEndpoint.getValue());

        replicaActor = actorSystem.actorSelection("akka.tcp://replica-" + String.valueOf(replicaEndpoint.getKey()) + "@" + replicaEndpoint.getValue() + "/user/front");
        //TODO: send first message to ensure the connection is established
        sessionOpen = true;
    }

    public Map.Entry<Integer, String> getReplicaEndpoint() {
        List<String> replicas = configuration.getAsList("replicas");
        return getRandomElement(replicas);
    }

    public Map.Entry<Integer, String> getRandomElement(List<String> list) {
        int replicaId = random.nextInt(list.size());
        return new AbstractMap.SimpleEntry<>(replicaId, list.get(replicaId));

    }

    public void closeSession() {
        sessionOpen = false;
    }

    public boolean isSessionOpen() {
        return sessionOpen;
    }

    public String read(String key) {

        if(key == null) {
            throw new RuntimeException("null key cannot be read");
        }

        ValueWrapper<String> valueWrapper = transactionMetadata.writtenValues.get(key);

        if(valueWrapper == null) {

            valueWrapper = transactionMetadata.readValues.get(key);

            if(valueWrapper == null) {

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

                    System.err.println("timestamp:" + readResponse.timestamp);

                    transactionMetadata.timestamp = readResponse.timestamp;
                }

                valueWrapper = new ValueWrapper<>(readResponse.value);
            }
        }

        transactionMetadata.readValues.put(key, valueWrapper);

        return valueWrapper.value;
    }

    public void write(String key, String value) {

        if(key == null) {
            throw new RuntimeException("null key cannot be written");
        }

        if(value == null) {
            throw new RuntimeException("null value cannot be written");
        }

        transactionMetadata.writtenValues.put(key, new ValueWrapper<>(value));
    }

    public void remove(String key) {

        if(key == null) {
            throw new RuntimeException("null key cannot be written");
        }

        transactionMetadata.writtenValues.put(key, new ValueWrapper<>(null));
    }

    public void clear() {
        transactionMetadata.clear();
    }

    public TransactionMetadata getTransactionMetadata() {
        return transactionMetadata;
    }
}
