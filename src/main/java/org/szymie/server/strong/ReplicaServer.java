package org.szymie.server.strong;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import org.springframework.beans.factory.DisposableBean;

public class ReplicaServer implements DisposableBean {

    private int port;
    private ServerChannelInitializer serverChannelInitializer;
    private int bossThreads;
    private int workerThreads;
    private EventLoopGroup bossGroup;
    private EventLoopGroup workerGroup;
    private ChannelFuture channelFuture;

    public ReplicaServer(int port, ServerChannelInitializer serverChannelInitializer, int bossThreads, int workerThreads) {
        this.port = port;
        this.serverChannelInitializer = serverChannelInitializer;
        this.bossThreads = bossThreads;
        this.workerThreads = workerThreads;
    }

    public void start() throws InterruptedException {

        //if(bossThreads != 0) {
            //bossGroup = new NioEventLoopGroup(bossThreads);
        //} else {
            bossGroup = new NioEventLoopGroup();
        //}

        //if(workerThreads != 0) {
            //workerGroup = new NioEventLoopGroup(workerThreads);
        //} else {
            workerGroup = new NioEventLoopGroup();
        //}

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