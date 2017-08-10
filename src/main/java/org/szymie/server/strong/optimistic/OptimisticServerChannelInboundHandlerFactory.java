package org.szymie.server.strong.optimistic;

import io.netty.channel.ChannelInboundHandler;
import org.szymie.server.strong.ChannelInboundHandlerFactory;

import java.util.concurrent.atomic.AtomicLong;

public class OptimisticServerChannelInboundHandlerFactory implements ChannelInboundHandlerFactory {

    private ResourceRepository resourceRepository;
    private AtomicLong timestamp;

    public OptimisticServerChannelInboundHandlerFactory(ResourceRepository resourceRepository, AtomicLong timestamp) {
        this.resourceRepository = resourceRepository;
        this.timestamp = timestamp;
    }

    @Override
    public ChannelInboundHandler create() {
        return new OptimisticServerMessageHandler(resourceRepository, timestamp);
    }
}
