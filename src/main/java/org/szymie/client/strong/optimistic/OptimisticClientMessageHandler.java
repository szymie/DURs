package org.szymie.client.strong.optimistic;

import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import org.szymie.messages.Messages;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

class OptimisticClientMessageHandler extends BaseClientMessageHandler {

    @Override
    public void channelRead0(ChannelHandlerContext ctx, Messages.Message msg) throws Exception {

        System.err.println("msg " + msg);

        switch(msg.getOneofMessagesCase()) {
            case READRESPONSE:
                setResponse(new Response<>(msg.getReadResponse()));
                break;
            case COMMITRESPONSE:
                setResponse(new Response<>(msg.getCommitResponse()));
                break;
            case INITRESPONSE:
                setResponse(new Response<>(msg.getInitResponse()));
        }
    }
}