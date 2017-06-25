package org.szymie.client;

import akka.actor.ActorSelection;
import akka.actor.ActorSystem;
import akka.pattern.Patterns;
import akka.util.Timeout;
import org.szymie.messages.CertificationRequest;
import org.szymie.messages.CertificationResponse;
import scala.concurrent.Await;
import scala.concurrent.Future;
import scala.concurrent.duration.Duration;
import java.util.Arrays;
import java.util.HashSet;
import java.util.concurrent.TimeUnit;

public class SerializableTransaction implements Transaction {

    private AkkaValueGateway valueGateway;
    private TransactionState state;
    private ActorSystem actorSystem;

    public SerializableTransaction(ActorSystem actorSystem) {
        this.valueGateway = new AkkaValueGateway(actorSystem);
        state = TransactionState.NOT_STARTED;
        this.actorSystem = actorSystem;
    }

    @Override
    public void begin() {
        checkStatus("", TransactionState.NOT_STARTED, TransactionState.COMMITTED, TransactionState.ABORTED);
        state = TransactionState.PROCESSING;
        valueGateway.clear();
    }

    private void checkStatus(String exceptionMessage, TransactionState... statuses) throws WrongTransactionStatus {

        HashSet<TransactionState> statusesSet = new HashSet<>(Arrays.asList(statuses));

        if(!statusesSet.contains(state)) {
            throw new WrongTransactionStatus(exceptionMessage);
        }
    }

    @Override
    public String read(String key) {

        checkStatus("", TransactionState.PROCESSING);

        if(!valueGateway.isSessionOpen()) {
            valueGateway.openSession();
        }

        return valueGateway.read(key);
    }

    @Override
    public void write(String key, String value) {
        checkStatus("", TransactionState.PROCESSING);
        valueGateway.write(key, value);
    }

    @Override
    public void remove(String key) {
        checkStatus("", TransactionState.PROCESSING);
        valueGateway.remove(key);
    }

    @Override
    public boolean commit() {

        checkStatus("", TransactionState.PROCESSING);

        state = TransactionState.TERMINATION;

        TransactionMetadata transactionMetadata = valueGateway.getTransactionMetadata();

        CertificationRequest request = new CertificationRequest(transactionMetadata.readValues, transactionMetadata.writtenValues, transactionMetadata.timestamp);

        CertificationResponse response;

        if(request.writtenValues.isEmpty()) {
            response = new CertificationResponse(true);
        } else {
            response = commitUpdateTransaction(request);
        }

        if(response.success) {
            state = TransactionState.COMMITTED;
        } else {
            state = TransactionState.ABORTED;
        }

        valueGateway.closeSession();

        return response.success;
    }

    private CertificationResponse commitUpdateTransaction(CertificationRequest request) {

        if(checkLocalCondition(request)) {

            Timeout timeout = new Timeout(Duration.create(5, TimeUnit.SECONDS));

            ActorSelection certifier = actorSystem.actorSelection(valueGateway.getReplicaPath() + "/certifier");

            Future<Object> future = Patterns.ask(certifier, request, timeout);

            try {
                return  (CertificationResponse) Await.result(future, timeout.duration());
            } catch (Exception ignore) { }
        }

        return new CertificationResponse(false);
    }

    private boolean checkLocalCondition(CertificationRequest request) {
        return request.readValues.entrySet()
                .stream()
                .allMatch(entry -> entry.getValue().fresh);
    }

    public TransactionState getState() {
        return state;
    }
}
