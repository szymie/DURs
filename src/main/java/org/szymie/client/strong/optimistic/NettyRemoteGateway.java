package org.szymie.client.strong.optimistic;

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioSocketChannel;
import org.springframework.beans.factory.DisposableBean;
import org.szymie.client.strong.pessimistic.RemoteGateway;
import org.szymie.messages.Messages;
import org.szymie.server.strong.ChannelInboundHandlerFactory;

public class NettyRemoteGateway implements DisposableBean, RemoteGateway {

    private Bootstrap bootstrap;
    private Channel channel;
    private EventLoopGroup workerGroup;
    private BaseClientMessageHandler handler;

    public  NettyRemoteGateway(ClientChannelInitializer clientChannelInitializer) {

        workerGroup = new NioEventLoopGroup();

        bootstrap = new Bootstrap();
        bootstrap.group(workerGroup);
        bootstrap.channel(NioSocketChannel.class);
        bootstrap.option(ChannelOption.SO_KEEPALIVE, true);
        bootstrap.handler(clientChannelInitializer);
    }

    public <T> void send(T object) {
        channel.writeAndFlush(object);
    }

    public <T, U> U sendAndReceive(T object, Class<U> returnType) {
        return handler.sendAndReceive(object, returnType);
    }

    public void connect(String endPoint) {

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

    @Override
    public void destroy() throws Exception {
        workerGroup.shutdownGracefully();
    }
}
