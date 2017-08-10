package org.szymie.client.strong.optimistic;

import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.socket.SocketChannel;
import io.netty.handler.codec.protobuf.ProtobufDecoder;
import io.netty.handler.codec.protobuf.ProtobufEncoder;
import io.netty.handler.codec.protobuf.ProtobufVarint32FrameDecoder;
import io.netty.handler.codec.protobuf.ProtobufVarint32LengthFieldPrepender;
import org.szymie.client.strong.ClientMessageHandlerFactory;
import org.szymie.messages.Messages;
import org.szymie.server.strong.ChannelInboundHandlerFactory;

public class ClientChannelInitializer extends ChannelInitializer<SocketChannel> {

    private ClientMessageHandlerFactory clientMessageHandlerFactory;

    public ClientChannelInitializer(ClientMessageHandlerFactory clientMessageHandlerFactory) {
        this.clientMessageHandlerFactory = clientMessageHandlerFactory;
    }

    @Override
    public void initChannel(SocketChannel ch) throws Exception {

        ChannelPipeline pipeline = ch.pipeline();

        pipeline.addLast(new ProtobufVarint32FrameDecoder());
        pipeline.addLast(new ProtobufDecoder(Messages.Message.getDefaultInstance()));
        pipeline.addLast(new ProtobufVarint32LengthFieldPrepender());
        pipeline.addLast(new ProtobufEncoder());
        pipeline.addLast(clientMessageHandlerFactory.create());
    }
}
