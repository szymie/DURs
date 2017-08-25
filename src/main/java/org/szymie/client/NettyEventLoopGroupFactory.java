package org.szymie.client;

import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;

public class NettyEventLoopGroupFactory {

    private EventLoopGroup eventLoopGroup;

    private NettyEventLoopGroupFactory(int numberOfThreads) {

        if(numberOfThreads != 0) {
            eventLoopGroup = new NioEventLoopGroup(numberOfThreads);
        } else {
            eventLoopGroup = new NioEventLoopGroup();
        }
    }

    private static volatile NettyEventLoopGroupFactory nettyEventLoopGroupFactory;

    public static EventLoopGroup getInstance(int numberOfThreads) {

        if(nettyEventLoopGroupFactory != null)
            return nettyEventLoopGroupFactory.eventLoopGroup;

        synchronized(NettyEventLoopGroupFactory.class) {
            if(nettyEventLoopGroupFactory == null) {
                nettyEventLoopGroupFactory = new NettyEventLoopGroupFactory(numberOfThreads);
            }
        }

        return nettyEventLoopGroupFactory.eventLoopGroup;
    }

    @Override
    protected void finalize() throws Throwable {
        eventLoopGroup.shutdownGracefully();
    }
}
