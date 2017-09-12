package org.szymie.server.strong.causal;

import com.google.common.collect.TreeMultiset;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import lsr.common.PID;
import lsr.paxos.client.ReplicationException;
import lsr.paxos.client.SerializableClient;
import org.szymie.BlockingMap;
import org.szymie.PaxosProcessesCreator;
import org.szymie.messages.CausalCertificationRequest;
import org.szymie.messages.CausalCertificationResponse;
import org.szymie.messages.Messages;
import org.szymie.server.strong.optimistic.ResourceRepository;
import org.szymie.server.strong.optimistic.ValueWithTimestamp;

import java.io.IOException;
import java.io.InputStream;
import java.util.*;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Lock;

public class CausalServerMessageHandler extends SimpleChannelInboundHandler<Messages.Message> implements PaxosProcessesCreator {

    private int id;

    private SerializableClient client;

    protected CausalResourceRepository resourceRepository;
    protected final AtomicLong timestamp;

    protected TreeMultiset<Long> liveTransactions;
    protected Lock liveTransactionsLock;

    private VectorClock vectorClock;

    private BlockingMap<Long, Long> responses;

    public CausalServerMessageHandler(int id, String paxosProcesses, CausalResourceRepository resourceRepository, AtomicLong timestamp, TreeMultiset<Long> liveTransactions, Lock liveTransactionsLock, VectorClock vectorClock, BlockingMap<Long, Long> responses) {

        this.id = id;
        this.resourceRepository = resourceRepository;
        this.timestamp = timestamp;
        this.liveTransactions = liveTransactions;
        this.liveTransactionsLock = liveTransactionsLock;
        this.vectorClock = vectorClock;
        this.responses = responses;

        List<PID> processes = createPaxosProcesses(paxosProcesses);

        InputStream paxosProperties = getClass().getClassLoader().getResourceAsStream("paxos.properties");

        try {

            if(processes.isEmpty()) {
                client = new SerializableClient(new lsr.common.Configuration(paxosProperties));
            } else {
                client = new SerializableClient(new lsr.common.Configuration(processes, paxosProperties));
            }

            client.connect();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    protected void channelRead0(ChannelHandlerContext ctx, Messages.Message msg) throws Exception {

        //System.err.println("causal " + msg);
        //System.err.println("message " + msg);

        switch (msg.getOneofMessagesCase()) {
            case INITREQUEST:
                handleInitRequest(ctx, msg.getInitRequest());
                break;
            case READREQUEST:
                //System.err.println("Read request from " + msg.getReadRequest().getTimestamp());
                handleReadRequest(ctx, msg.getReadRequest());
                break;
            case COMMITREQUEST:
                handleCommitRequest(ctx, msg.getCommitRequest());
                break;
        }
    }

    private void handleInitRequest(ChannelHandlerContext context, Messages.InitRequest initRequest) {

        resourceRepository.clear();

        long time = timestamp.incrementAndGet();
        initRequest.getWritesMap().forEach((key, value) -> resourceRepository.put(key, value, 0, time));

        Messages.InitResponse initResponse = Messages.InitResponse.newBuilder()
                .build();

        Messages.Message response = Messages.Message.newBuilder()
                .setInitResponse(initResponse)
                .build();

        context.writeAndFlush(response);
    }

    protected void handleReadRequest(ChannelHandlerContext context, Messages.ReadRequest request) {


        boolean firstRead = request.getTimestamp() == Long.MAX_VALUE;

        if(firstRead) {

            long requiredLocalClock = request.getLocalClock();

            if(requiredLocalClock > timestamp.get()) {

                try {
                    synchronized(timestamp) {
                        while(requiredLocalClock < timestamp.get()) {
                            timestamp.wait();
                        }
                    }
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }
        }

        long transactionTimestamp = firstRead ? timestamp.get() : request.getTimestamp();

        if (firstRead) {
            liveTransactionsLock.lock();
            liveTransactions.add(transactionTimestamp);
            liveTransactionsLock.unlock();
        }

        Optional<ValuesWithTimestamp<String>> valueOptional = resourceRepository.get(request.getKey(), transactionTimestamp);

        Messages.CausalReadResponse response = valueOptional.map(valueWithTimestamp ->
                createCausalReadResponse(valueWithTimestamp.values, transactionTimestamp, valueWithTimestamp.fresh))
                .orElse(createCausalReadResponse(Collections.emptyList(), transactionTimestamp, true));

        Messages.Message message = Messages.Message.newBuilder()
                .setCausalReadResponse(response)
                .build();

        context.writeAndFlush(message);
    }

    private Messages.CausalReadResponse createCausalReadResponse(List<String> values, long timestamp, boolean fresh) {
        return Messages.CausalReadResponse.newBuilder()
                .addAllValues(values)
                .setTimestamp(timestamp)
                .setFresh(fresh).build();
    }

    private void handleCommitRequest(ChannelHandlerContext context, Messages.CommitRequest request) {

        if(request.getWritesMap().isEmpty()) {
            commitReadOnlyTransaction(context, request);
        } else {

            long commitTimestamp;

            try {
                CausalCertificationResponse response = (CausalCertificationResponse) client.execute(new CausalCertificationRequest(id, new HashMap<>(request.getWritesMap()),
                        request.getTimestamp(), vectorClock.getCopy()));
                commitTimestamp = responses.get(response.sequentialNumber);
                responses.remove(response.sequentialNumber);
            } catch (IOException | ClassNotFoundException | ReplicationException e) {
                e.printStackTrace();
                throw new RuntimeException(e);
            }

            Messages.CommitResponse response = Messages.CommitResponse.newBuilder().setTimestamp(commitTimestamp).build();
            Messages.Message message = Messages.Message.newBuilder().setCommitResponse(response).build();

            context.writeAndFlush(message);
        }
    }

    protected void commitReadOnlyTransaction(ChannelHandlerContext context, Messages.CommitRequest request) {

        liveTransactionsLock.lock();
        liveTransactions.remove(request.getTimestamp());
        liveTransactionsLock.unlock();

        Messages.CommitResponse response = Messages.CommitResponse.newBuilder().build();
        Messages.Message message = Messages.Message.newBuilder().setCommitResponse(response).build();

        context.writeAndFlush(message);
    }
}