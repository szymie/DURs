package org.szymie.client;

import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;

public class NettyEventLoopGroupFactory2 {

    private NettyEventLoopGroupFactory2 () {}

    private static volatile EventLoopGroup eventLoopGroup;

    public static EventLoopGroup getInstance(int numberOfThreads) {

        if(eventLoopGroup != null)
            return eventLoopGroup;

        synchronized(NettyEventLoopGroupFactory2.class) {
            if (eventLoopGroup == null ) {

                if(numberOfThreads != 0) {
                    eventLoopGroup = new NioEventLoopGroup(numberOfThreads);
                } else {
                    eventLoopGroup = new NioEventLoopGroup();
                }
            }
        }

        return eventLoopGroup;
    }

    @Override
    protected void finalize() throws Throwable {
        eventLoopGroup.shutdownGracefully();
    }
}
