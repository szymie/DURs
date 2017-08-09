package org.szymie.server.strong;

import io.netty.channel.ChannelInboundHandler;

public interface ChannelInboundHandlerFactory {
    ChannelInboundHandler create();
}
