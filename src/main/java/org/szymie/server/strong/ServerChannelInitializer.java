package org.szymie.server.strong;

import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.socket.SocketChannel;
import io.netty.handler.codec.protobuf.ProtobufDecoder;
import io.netty.handler.codec.protobuf.ProtobufEncoder;
import io.netty.handler.codec.protobuf.ProtobufVarint32FrameDecoder;
import io.netty.handler.codec.protobuf.ProtobufVarint32LengthFieldPrepender;
import org.szymie.messages.Messages;
import org.szymie.server.strong.ChannelInboundHandlerFactory;

public class ServerChannelInitializer extends ChannelInitializer<SocketChannel> {

    private ChannelInboundHandlerFactory channelInboundHandlerFactory;

    public ServerChannelInitializer(ChannelInboundHandlerFactory channelInboundHandlerFactory) {
        this.channelInboundHandlerFactory = channelInboundHandlerFactory;
    }

    @Override
    public void initChannel(SocketChannel ch) throws Exception {

        ChannelPipeline pipeline = ch.pipeline();

        pipeline.addLast(new ProtobufVarint32FrameDecoder());
        pipeline.addLast(new ProtobufDecoder(Messages.Message.getDefaultInstance()));
        pipeline.addLast(new ProtobufVarint32LengthFieldPrepender());
        pipeline.addLast(new ProtobufEncoder());
        pipeline.addLast(channelInboundHandlerFactory.create());
    }
}
