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

    private TreeMultiset<Long> liveTransactions;
    private Lock liveTransactionsLock;

    public PessimisticServerMessageHandler(int id, String paxosProcesses, ResourceRepository resourceRepository, AtomicLong timestamp,
                                           BlockingMap<Long, BlockingQueue<ChannelHandlerContext>> contexts,
                                           Map<Long, TransactionMetadata> activeTransactions,
                                           BlockingMap<Long, Boolean> activeTransactionFlags,
                                           TreeMultiset<Long> liveTransactions, Lock liveTransactionsLock) {

        super(resourceRepository, timestamp);

        this.id = id;
        this.contexts = contexts;
        this.activeTransactions = activeTransactions;

        this.liveTransactions = liveTransactions;
        this.liveTransactionsLock = liveTransactionsLock;

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

        this.activeTransactionFlags = activeTransactionFlags;
    }

    @Override
    public void channelRead0(ChannelHandlerContext ctx, Messages.Message msg) throws Exception {

        System.err.println("msg");

        super.channelRead0(ctx, msg);

        switch (msg.getOneofMessagesCase()) {
            case BEGINTRANSACTIONREQUEST:
                handleBeginTransactionRequest(ctx, msg.getBeginTransactionRequest());
                break;
            case READREQUEST:
                System.err.println("Read request from " + msg.getReadRequest().getTimestamp());
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

            System.err.println("for " + response.getTimestamp() + " context should have been set at " + id);

            BlockingQueue<ChannelHandlerContext> contextHolder = contexts.get(response.getTimestamp());

            try {
                contextHolder.put(context);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }

            if(response.getStartPossible()) {

                Messages.Message message = Messages.Message.newBuilder()
                        .setBeginTransactionResponse(response)
                        .build();

                System.err.println("want to tell that " + response.getTimestamp() + " can start");
                context.writeAndFlush(message);
                System.err.println("told that " + response.getTimestamp() + " can start");
            }
        } catch (IOException | ClassNotFoundException | ReplicationException e) {
            e.printStackTrace();
            throw new RuntimeException(e);
        }
    }

    private void handleReadRequest(ChannelHandlerContext context, Messages.ReadRequest request) {

        boolean firstRead = request.getTimestamp() == Long.MAX_VALUE;

        long transactionTimestamp = firstRead ? timestamp.get() : request.getTimestamp();

        if(firstRead) {
            liveTransactionsLock.lock();
            liveTransactions.add(transactionTimestamp);
            liveTransactionsLock.unlock();
        }

        Optional<ValueWithTimestamp> valueOptional = resourceRepository.get(request.getKey(), transactionTimestamp);

        Messages.ReadResponse response = valueOptional.map(valueWithTimestamp ->
                createReadResponse(valueWithTimestamp.value, transactionTimestamp, valueWithTimestamp.fresh))
                .orElse(createReadResponse("", transactionTimestamp, true));

        Messages.Message message = Messages.Message.newBuilder()
                .setReadResponse(response)
                .build();

        context.writeAndFlush(message);
    }

    private Messages.ReadResponse createReadResponse(String value, long timestamp, boolean fresh) {
        return Messages.ReadResponse.newBuilder()
                .setValue(value)
                .setTimestamp(timestamp)
                .setFresh(fresh).build();
    }

    private void handleCommitRequest(ChannelHandlerContext context, Messages.CommitRequest request) {

        if(request.getWritesMap().isEmpty()) {

            liveTransactionsLock.lock();
            liveTransactions.remove(request.getTimestamp());
            liveTransactionsLock.unlock();

            Messages.CommitResponse response = Messages.CommitResponse.newBuilder().build();
            Messages.Message message = Messages.Message.newBuilder().setCommitResponse(response).build();

            context.writeAndFlush(message);
        } else {

            activeTransactionFlags.get(request.getTimestamp());
            TransactionMetadata transaction = activeTransactions.get(request.getTimestamp());

            Messages.StateUpdateRequest stateUpdateRequest = Messages.StateUpdateRequest.newBuilder()
                    .setTimestamp(request.getTimestamp())
                    .setApplyAfter(transaction.getApplyAfter())
                    .putAllWrites(request.getWritesMap())
                    .build();

            Messages.Message message = Messages.Message
                    .newBuilder()
                    .setStateUpdateRequest(stateUpdateRequest)
                    .build();

            System.err.println("timestamp: " + timestamp);
            System.err.println("transaction: " + transaction);

            try {
                client.execute(message);
            } catch (IOException | ClassNotFoundException | ReplicationException e) {
                e.printStackTrace();
            }
        }
    }


    /*@Override
    public void channelActive(ChannelHandlerContext ctx) throws Exception {

        ByteBuf time = ctx.alloc().buffer(4);
        time.writeInt((int) (System.currentTimeMillis() / 1000L + 2208988800L));

        ChannelFuture f = ctx.writeAndFlush(time);

        //f.sync();

        //ctx.close();

        f.addListener(ChannelFutureListener.CLOSE);

        f.addListener((ChannelFutureListener) future -> {
            assert f == future;
            ctx.close();
        });
    }*/



    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        cause.printStackTrace();
        ctx.close();
    }
}
