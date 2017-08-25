package org.szymie.client.strong.optimistic;

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.*;
import io.netty.channel.socket.nio.NioSocketChannel;
import org.szymie.client.NettyEventLoopGroupFactory;
import org.szymie.client.strong.pessimistic.RemoteGateway;

public class NettyRemoteGateway implements RemoteGateway {

    private Bootstrap bootstrap;
    private Channel channel;
    private EventLoopGroup workerGroup;
    private BaseClientMessageHandler handler;
    private ClientChannelInitializer clientChannelInitializer;

    private int numberOfClientThreads;

    public NettyRemoteGateway(ClientChannelInitializer clientChannelInitializer) {
        this(0, clientChannelInitializer);
    }

    public NettyRemoteGateway(int numberOfClientThreads, ClientChannelInitializer clientChannelInitializer) {
        this.numberOfClientThreads = numberOfClientThreads;
        this.clientChannelInitializer = clientChannelInitializer;
    }

    public <T> void send(T object) {
        channel.writeAndFlush(object);
    }

    public <T, U> U sendAndReceive(T object, Class<U> returnType) {
        return handler.sendAndReceive(object, returnType);
    }

    public void connect(String endPoint) {

        workerGroup = NettyEventLoopGroupFactory.getInstance(numberOfClientThreads);

        bootstrap = new Bootstrap();
        bootstrap.group(workerGroup);
        bootstrap.channel(NioSocketChannel.class);
        bootstrap.option(ChannelOption.SO_KEEPALIVE, true);
        bootstrap.handler(clientChannelInitializer);

        String[] addressAndPort = endPoint.split(":");

        try {
            channel = bootstrap.connect(addressAndPort[0], Integer.parseInt(addressAndPort[1])).sync().channel();
            handler =  channel.pipeline().get(BaseClientMessageHandler.class);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }


    public void disconnect() {
        try {
            channel.close().sync();
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }
}
