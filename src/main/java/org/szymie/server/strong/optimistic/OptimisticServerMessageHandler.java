package org.szymie.server.strong.optimistic;

import com.google.common.collect.TreeMultiset;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import lsr.common.PID;
import lsr.paxos.client.ReplicationException;
import lsr.paxos.client.SerializableClient;
import org.szymie.PaxosProcessesCreator;
import org.szymie.messages.CertificationRequest;
import org.szymie.messages.CertificationResponse;
import org.szymie.messages.Messages;
import org.szymie.server.strong.BaseServerMessageHandler;

import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Lock;
import java.util.stream.Collectors;

public class OptimisticServerMessageHandler extends BaseServerMessageHandler implements PaxosProcessesCreator {

    private int id;
    private SerializableClient client;
    private boolean connected;

    public OptimisticServerMessageHandler(int id, String paxosProcesses, ResourceRepository resourceRepository, AtomicLong timestamp,
                                          TreeMultiset<Long> liveTransactions, Lock liveTransactionsLock) {
        super(resourceRepository, timestamp, liveTransactions, liveTransactionsLock);

        this.id = id;

        List<PID> processes = createPaxosProcesses(paxosProcesses);

        try {

            if(processes.isEmpty()) {
                InputStream paxosProperties = getClass().getClassLoader().getResourceAsStream("paxos.properties");
                client = new SerializableClient(new lsr.common.Configuration(paxosProperties), id);
            } else {
                client = new SerializableClient(new lsr.common.Configuration(processes), id);
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void channelRead0(ChannelHandlerContext ctx, Messages.Message msg) throws Exception {

        super.channelRead0(ctx, msg);

        switch (msg.getOneofMessagesCase()) {
            case READREQUEST:
                handleReadRequest(ctx, msg.getReadRequest());
                break;
            case COMMITREQUEST:
                handleCommitRequest(ctx, msg.getCommitRequest());
                break;
            case CERTIFICATIONREQUEST:
                handleCertificationRequest(ctx, msg.getCertificationRequest());
                break;
        }
    }

    private void handleCertificationRequest(ChannelHandlerContext context, Messages.CertificationRequest certificationRequest) {

        CertificationResponse certificationResponse;

        try {

            if(!connected) {
                client.connect();
                connected = true;
            }

            Map<String, ValueWithTimestamp<String>> reads = certificationRequest.getReadsMap().entrySet().stream().collect(Collectors.toMap(Map.Entry::getKey, entry -> new ValueWithTimestamp<>(entry.getValue(), 0, true)));
            Map<String, ValueWithTimestamp<String>> writes = certificationRequest.getWritesMap().entrySet().stream().collect(Collectors.toMap(Map.Entry::getKey, entry -> new ValueWithTimestamp<>(entry.getValue(), 0, true)));

            certificationResponse = (CertificationResponse) client.execute(new CertificationRequest(reads, writes, certificationRequest.getTimestamp()));

        } catch (IOException | ClassNotFoundException | ReplicationException e) {
            e.printStackTrace();
            throw new RuntimeException(e);
        }

        Messages.CertificationResponse response = Messages.CertificationResponse.newBuilder().setSuccess(certificationResponse.success).build();
        Messages.Message message = Messages.Message.newBuilder().setCertificationResponse(response).build();

        context.writeAndFlush(message);
    }

    private void handleCommitRequest(ChannelHandlerContext context, Messages.CommitRequest request) {
        commitReadOnlyTransaction(context, request);
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        cause.printStackTrace();
        ctx.close();
    }
}
