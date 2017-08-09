package org.szymie.client.strong.optimistic;

import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import org.szymie.messages.Messages;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

class NettyClientMessageHandler extends SimpleChannelInboundHandler<Messages.Message> {

    private Channel channel;
    private BlockingQueue<Response> responses = new LinkedBlockingQueue<>();

    public <T> void send(T object) {
        channel.writeAndFlush(object);
    }

    public <T, U> U sendAndReceive(T object, Class<U> returnType) {
        channel.writeAndFlush(object);
        return getResponse(returnType);
    }

    @Override
    public void channelRead0(ChannelHandlerContext ctx, Messages.Message msg) throws Exception {

        System.err.println("msg " + msg);

        switch(msg.getOneofMessagesCase()) {
            case READRESPONSE:
                Messages.ReadResponse readResponse = msg.getReadResponse();
                setResponse(new Response<>(readResponse));
                break;
        }
    }

    @Override
    public void channelRegistered(ChannelHandlerContext ctx) throws Exception {
        channel = ctx.channel();
    }

    private <T> void setResponse(Response<T> response) {
        responses.add(response);
    }

    private <T> T getResponse(Class<T> returnType) {

        try {
            Response take = responses.take();
            return returnType.cast(take.value);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    private class Response<T> {

        T value;

        public Response(T value) {
            this.value = value;
        }

        void setValue(T value) {
            this.value = value;
        }
    }
}