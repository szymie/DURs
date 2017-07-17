package org.szymie.client.strong.optimistic;

import akka.actor.ActorSelection;
import akka.actor.ActorSystem;
import akka.pattern.Patterns;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.szymie.Configuration;

import java.util.*;
import java.util.concurrent.TimeUnit;

import org.szymie.messages.ReadRequest;
import org.szymie.messages.ReadResponse;
import org.szymie.server.strong.optimistic.ValueWithTimestamp;
import scala.concurrent.Future;
import scala.concurrent.Await;
import akka.util.Timeout;
import scala.concurrent.duration.Duration;


public class AkkaValueGateway implements ValueGateway {

    private ActorSystem actorSystem;
    private TransactionData transactionData;
    private Configuration configuration;
    private boolean sessionOpen;
    private int readTimeout;

    private ActorSelection replicaActor;

    private Logger logger = LoggerFactory.getLogger(AkkaValueGateway.class);

    public AkkaValueGateway(ActorSystem actorSystem) {

        this.actorSystem = actorSystem;
        transactionData = new TransactionData();
        configuration = new Configuration();
        sessionOpen = false;
        readTimeout = Integer.parseInt(configuration.get("read_timeout"));
    }

    public void openSession() {

        Map.Entry<Integer, String> replicaEndpoint = configuration.getRandomReplicaEndpoint();

        System.err.println(replicaEndpoint.getKey() + " " + replicaEndpoint.getValue());

        replicaActor = actorSystem.actorSelection("akka.tcp://replica-" + replicaEndpoint.getKey() + "@" + replicaEndpoint.getValue() + "/user/front");
        //TODO: send first message to ensure the connection is established

        sessionOpen = true;
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

        ValueWithTimestamp value = transactionData.writtenValues.get(key);

        if(value == null) {

            value = transactionData.readValues.get(key);

            if(value == null) {
                ReadResponse readResponse = readRemotely(key);
                value = new ValueWithTimestamp(readResponse.value, readResponse.timestamp, readResponse.fresh);
            }
        }

        transactionData.readValues.put(key, value);

        return value.value;
    }
    
    private ReadResponse readRemotely(String key) {

        ReadRequest readRequest = new ReadRequest(key, transactionData.timestamp);
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

        transactionData.writtenValues.put(key, new ValueWithTimestamp(value, Long.MAX_VALUE, true));
    }

    public void remove(String key) {

        if(key == null) {
            throw new RuntimeException("empty key cannot be written");
        }

        transactionData.writtenValues.put(key, new ValueWithTimestamp(null, Long.MAX_VALUE, true));
    }

    public void clear() {
        transactionData.clear();
    }

    public TransactionData getTransactionData() {
        return transactionData;
    }
}
