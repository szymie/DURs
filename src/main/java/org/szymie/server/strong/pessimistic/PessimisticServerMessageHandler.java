package org.szymie.server.strong.pessimistic;

import com.google.common.collect.TreeMultiset;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import lsr.common.PID;
import lsr.paxos.client.ReplicationException;
import lsr.paxos.client.SerializableClient;
import org.szymie.BlockingMap;
import org.szymie.PaxosProcessesCreator;
import org.szymie.messages.BeginTransactionResponse;
import org.szymie.messages.Messages;
import org.szymie.messages.StateUpdate;
import org.szymie.server.strong.BaseServerMessageHandler;
import org.szymie.server.strong.optimistic.ResourceRepository;
import org.szymie.server.strong.optimistic.ValueWithTimestamp;

import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Lock;

public class PessimisticServerMessageHandler extends BaseServerMessageHandler implements PaxosProcessesCreator {

    private int id;
    private SerializableClient client;
    private BlockingMap<Long, BlockingQueue<ChannelHandlerContext>> contexts;

    private Map<Long, TransactionMetadata> activeTransactions;
    private BlockingMap<Long, Boolean> activeTransactionFlags;

    private GroupMessenger groupMessenger;

    public PessimisticServerMessageHandler(int id, String paxosProcesses, ResourceRepository resourceRepository,
                                           BlockingMap<Long, BlockingQueue<ChannelHandlerContext>> contexts,
                                           Map<Long, TransactionMetadata> activeTransactions,
                                           BlockingMap<Long, Boolean> activeTransactionFlags,
                                           TreeMultiset<Long> liveTransactions, Lock liveTransactionsLock,
                                           GroupMessenger groupMessenger,
                                           AtomicLong lastCommitted) {

        super(resourceRepository, lastCommitted, liveTransactions, liveTransactionsLock);

        this.id = id;
        this.contexts = contexts;
        this.activeTransactions = activeTransactions;

        this.liveTransactions = liveTransactions;
        this.liveTransactionsLock = liveTransactionsLock;

        List<PID> processes = createPaxosProcesses(paxosProcesses);

        try {

            if(processes.isEmpty()) {
                InputStream paxosProperties = getClass().getClassLoader().getResourceAsStream("paxos.properties");
                client = new SerializableClient(new lsr.common.Configuration(paxosProperties), id);
            } else {
                client = new SerializableClient(new lsr.common.Configuration(processes), id);
            }

            client.connect();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        this.activeTransactionFlags = activeTransactionFlags;

        this.groupMessenger = groupMessenger;
    }

    @Override
    public void channelRead0(ChannelHandlerContext ctx, Messages.Message msg) throws Exception {

        super.channelRead0(ctx, msg);

        switch (msg.getOneofMessagesCase()) {
            case BEGINTRANSACTIONREQUEST:
                handleBeginTransactionRequest(ctx, msg.getBeginTransactionRequest());
                break;
            case READREQUEST:
                handleReadRequest(ctx, msg.getReadRequest());
                break;
            case COMMITREQUEST:
                handleCommitRequest(ctx, msg.getCommitRequest());
                break;
        }
    }

    private void handleBeginTransactionRequest(ChannelHandlerContext context, Messages.BeginTransactionRequest request) {

        try {

            Messages.BeginTransactionRequest requestWithId = Messages.BeginTransactionRequest.newBuilder(request)
                    .setId(id)
                    .build();

            Messages.BeginTransactionResponse response = (Messages.BeginTransactionResponse) client.execute(Messages.Message.newBuilder()
                    .setBeginTransactionRequest(requestWithId)
                    .build());

            liveTransactionsLock.lock();
            liveTransactions.add(response.getTimestamp());
            liveTransactionsLock.unlock();

            boolean startPossible = activeTransactionFlags.get(response.getTimestamp());

            //System.err.println("for " + response.getTimestamp() + " context should have been set at " + id);

            BlockingQueue<ChannelHandlerContext> contextHolder = contexts.get(response.getTimestamp());

            //System.err.println("for " + response.getTimestamp() + " context holder get at " + id);

            try {
                contextHolder.put(context);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }

            //System.err.println("for " + response.getTimestamp() + " context holder filled at " + id);

            //System.err.println("for " + response.getTimestamp() + " start possible " + startPossible);

            if(startPossible) {

                Messages.BeginTransactionResponse beginTransactionResponse = Messages.BeginTransactionResponse.newBuilder()
                        .setTimestamp(response.getTimestamp())
                        .setStartPossible(true)
                        .build();

                Messages.Message message = Messages.Message.newBuilder()
                        .setBeginTransactionResponse(beginTransactionResponse)
                        .build();

                //System.err.println("want to tell that " + response.getTimestamp() + " can start");
                context.writeAndFlush(message);
                //System.err.println("told that " + response.getTimestamp() + " can start");
            } else {
                //System.err.println("for " + response.getTimestamp() + " didn't start");
            }
        } catch (IOException | ClassNotFoundException | ReplicationException e) {
            e.printStackTrace();
            throw new RuntimeException(e);
        }
    }

    private void handleCommitRequest(ChannelHandlerContext context, Messages.CommitRequest request) {

        if(request.getWritesMap().isEmpty()) {
            commitReadOnlyTransaction(context, request);
        } else {
            //activeTransactionFlags.get(request.getTimestamp());
            //System.err.println("getting transaction from active " + request.getTimestamp());
            TransactionMetadata transaction = activeTransactions.get(request.getTimestamp());
            //System.err.println("got transaction from active " + request.getTimestamp());
            groupMessenger.send(new StateUpdate(request.getTimestamp(), transaction.getApplyAfter(), new HashMap<>(request.getWritesMap())));
            //System.err.println("Sent state update for " + request.getTimestamp());
        }
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        cause.printStackTrace();
        ctx.close();
    }
}
