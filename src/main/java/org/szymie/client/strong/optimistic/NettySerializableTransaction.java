package org.szymie.client.strong.optimistic;

import lsr.common.PID;
import lsr.paxos.client.ReplicationException;
import lsr.paxos.client.SerializableClient;
import org.szymie.Configuration;
import org.szymie.PaxosProcessesCreator;
import org.szymie.messages.CertificationRequest;
import org.szymie.messages.CertificationResponse;

import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;

public class NettySerializableTransaction implements Transaction, PaxosProcessesCreator {

    private NettyValueGateway valueGateway;
    private TransactionState state;
    private SerializableClient client;

    public NettySerializableTransaction() {
        this(new Configuration());
    }

    public NettySerializableTransaction(Configuration configuration) {

        this.valueGateway = new NettyValueGateway(new NettyRemoteGateway(new ClientChannelInitializer(new OptimisticClientMessageHandlerFactory())), configuration);
        state = TransactionState.NOT_STARTED;

        String paxosProcesses = configuration.get("paxosProcesses", "");
        List<PID> processes = createPaxosProcesses(paxosProcesses);

        InputStream paxosProperties = getClass().getClassLoader().getResourceAsStream("paxos.properties");

        try {
            if(processes.isEmpty()) {
                client = new SerializableClient(new lsr.common.Configuration(paxosProperties));
            } else {
                client = new SerializableClient(new lsr.common.Configuration(processes, paxosProperties));
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
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

        TransactionData transactionData = valueGateway.getTransactionData();

        CertificationRequest request = new CertificationRequest(transactionData.readValues, transactionData.writtenValues, transactionData.timestamp);

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

            client.connect();

            try {
                return (CertificationResponse) client.execute(request);
            } catch (IOException | ClassNotFoundException | ReplicationException e) {
                e.printStackTrace();
                throw new RuntimeException(e);
            }
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
