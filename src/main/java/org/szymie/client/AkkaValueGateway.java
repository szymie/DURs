package org.szymie.client;

import akka.actor.ActorSelection;
import akka.actor.ActorSystem;
import akka.pattern.Patterns;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
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


public class AkkaValueGateway implements ValueGateway {

    private ActorSystem actorSystem;
    private TransactionMetadata transactionMetadata;
    private Configuration configuration;
    private boolean sessionOpen;
    private int readTimeout;

    private Random random;
    private ActorSelection replicaActor;

    private Logger logger = LoggerFactory.getLogger(AkkaValueGateway.class);

    public AkkaValueGateway(ActorSystem actorSystem) {

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

        replicaActor = actorSystem.actorSelection("akka.tcp://replica-" + replicaEndpoint.getKey() + "@" + replicaEndpoint.getValue() + "/user/front");
        //TODO: send first message to ensure the connection is established

        sessionOpen = true;
    }

    private Map.Entry<Integer, String> getReplicaEndpoint() {
        List<String> replicas = configuration.getAsList("replicas");
        return getRandomElement(replicas);
    }

    private Map.Entry<Integer, String> getRandomElement(List<String> list) {
        int index = random.nextInt(list.size());
        String[] replica = list.get(index).split("-");
        return new AbstractMap.SimpleEntry<>(Integer.parseInt(replica[0]), replica[1]);
    }

    public void closeSession() {
        sessionOpen = false;
    }

    public boolean isSessionOpen() {
        return sessionOpen;
    }

    public String read(String key) {

        if(key == null) {
            throw new RuntimeException("empty key cannot be read");
        }

        ValueWrapper<String> valueWrapper = transactionMetadata.writtenValues.get(key);

        if(valueWrapper == null) {

            valueWrapper = transactionMetadata.readValues.get(key);

            if(valueWrapper == null) {
                ReadResponse readResponse = readRemotely(key);
                valueWrapper = new ValueWrapper<>(readResponse.value);
            }
        }

        transactionMetadata.readValues.put(key, valueWrapper);

        return valueWrapper.value;
    }
    
    private ReadResponse readRemotely(String key) {

        ReadRequest readRequest = new ReadRequest(key, transactionMetadata.timestamp);
        Timeout timeout = new Timeout(Duration.create(readTimeout, TimeUnit.SECONDS));

        ReadResponse readResponse = null;

        while(readResponse == null) {

            Future<Object> future = Patterns.ask(replicaActor, readRequest, timeout);

            try {
                readResponse = (ReadResponse) Await.result(future, timeout.duration());
            } catch (Exception e) {
                logger.error(e.getMessage(), e);
                openSession();
            }
        }

        return readResponse;
    }

    public void write(String key, String value) {

        if(key == null) {
            throw new RuntimeException("empty key cannot be written");
        }

        if(value == null) {
            throw new RuntimeException("empty value cannot be written");
        }

        transactionMetadata.writtenValues.put(key, new ValueWrapper<>(value));
    }

    public void remove(String key) {

        if(key == null) {
            throw new RuntimeException("empty key cannot be written");
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
