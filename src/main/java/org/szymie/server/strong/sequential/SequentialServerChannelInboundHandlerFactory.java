package org.szymie.server.strong.sequential;


import io.netty.channel.ChannelInboundHandler;
import org.szymie.server.strong.ChannelInboundHandlerFactory;
import org.szymie.server.strong.optimistic.ResourceRepository;

public class SequentialServerChannelInboundHandlerFactory implements ChannelInboundHandlerFactory {

    private String paxosProcesses;

    public SequentialServerChannelInboundHandlerFactory(String paxosProcesses) {
        this.paxosProcesses = paxosProcesses;
    }

    @Override
    public ChannelInboundHandler create() {
        return new SequentialServerMessageHandler(paxosProcesses);
    }
}
