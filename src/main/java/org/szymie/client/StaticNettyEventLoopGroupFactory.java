package org.szymie.client;

import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;

public class StaticNettyEventLoopGroupFactory {

    private StaticNettyEventLoopGroupFactory() {
    }

    private static class Holder {
        private static final EventLoopGroup INSTANCE = new NioEventLoopGroup();
    }

    public static EventLoopGroup getInstance() {
        return Holder.INSTANCE;
    }

    @Override
    protected void finalize() throws Throwable {
        Holder.INSTANCE.shutdownGracefully();
    }
}


