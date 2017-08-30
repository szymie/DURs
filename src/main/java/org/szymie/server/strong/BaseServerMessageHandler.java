package org.szymie.server.strong;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import org.szymie.ValueWrapper;
import org.szymie.messages.Messages;
import org.szymie.server.strong.optimistic.ResourceRepository;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;

public abstract class BaseServerMessageHandler extends SimpleChannelInboundHandler<Messages.Message> {

    protected ResourceRepository resourceRepository;
    protected AtomicLong timestamp;

    public BaseServerMessageHandler(ResourceRepository resourceRepository, AtomicLong timestamp) {
        this.resourceRepository = resourceRepository;
        this.timestamp = timestamp;
    }



    @Override
    protected void channelRead0(ChannelHandlerContext ctx, Messages.Message msg) throws Exception {

        System.err.println("message " + msg);

        switch (msg.getOneofMessagesCase()) {
            case INITREQUEST:
                handleInitRequest(ctx, msg.getInitRequest());
                break;
            case SHOWRESOURCEREQUEST:

                Set<String> keys = resourceRepository.getKeys();

                keys.forEach(key -> {
                    System.out.println("key: "+ key);
                    Map<Long, ValueWrapper<String>> versions = resourceRepository.getAll(key);
                    versions.forEach((version, value) -> System.out.println("\tversion: "+ version + ", value: "+ value));
                });

                break;
        }
    }

    private void handleInitRequest(ChannelHandlerContext context, Messages.InitRequest initRequest) {

        resourceRepository.clear();

        long time = timestamp.incrementAndGet();
        initRequest.getWritesMap().forEach((key, value) ->  resourceRepository.put(key, value, time));

        Messages.InitResponse initResponse = Messages.InitResponse.newBuilder()
                .build();

        Messages.Message response = Messages.Message.newBuilder()
                .setInitResponse(initResponse)
                .build();

        context.writeAndFlush(response);
    }
}