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
                Messages.ReadResponse readResponse = msg.getReadResponse();
                setResponse(new Response<>(readResponse));
                break;
            case COMMITRESPONSE:
                Messages.CommitResponse commitResponse = msg.getCommitResponse();
                setResponse(new Response<>(commitResponse));
                break;
            case INITRESPONSE:
                Messages.InitResponse initResponse = msg.getInitResponse();
                setResponse(new Response<>(initResponse));
        }
    }
}