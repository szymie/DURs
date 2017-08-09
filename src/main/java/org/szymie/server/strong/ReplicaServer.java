package org.szymie.server.strong;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import org.springframework.beans.factory.DisposableBean;

public class ReplicaServer implements DisposableBean {

    private int port;
    private ServerChannelInitializer serverChannelInitializer;
    private EventLoopGroup bossGroup;
    private EventLoopGroup workerGroup = new NioEventLoopGroup();
    private ChannelFuture channelFuture;

    public ReplicaServer(int port, ServerChannelInitializer serverChannelInitializer) {
        this.port = port;
        this.serverChannelInitializer = serverChannelInitializer;
    }

    public void start() throws InterruptedException {

        bossGroup = new NioEventLoopGroup();
        workerGroup = new NioEventLoopGroup();

        ServerBootstrap bootstrap = new ServerBootstrap();

        bootstrap.group(bossGroup, workerGroup)
                .channel(NioServerSocketChannel.class)
                .childHandler(serverChannelInitializer)
                .option(ChannelOption.SO_BACKLOG, 128)
                .childOption(ChannelOption.SO_KEEPALIVE, true);

        channelFuture = bootstrap.bind(port).sync();
    }

    @Override
    public void destroy() throws Exception {
        workerGroup.shutdownGracefully();
        bossGroup.shutdownGracefully();
        channelFuture.channel().closeFuture().sync();
    }
}