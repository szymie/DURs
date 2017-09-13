package org.szymie.client.strong.causal;

import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import org.szymie.client.strong.optimistic.BaseClientMessageHandler;
import org.szymie.messages.Messages;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

class CausalClientMessageHandler extends BaseClientMessageHandler {

    @Override
    public void channelRead0(ChannelHandlerContext ctx, Messages.Message msg) throws Exception {

        switch(msg.getOneofMessagesCase()) {
            case CAUSALREADRESPONSE:
                setResponse(new Response<>(msg.getCausalReadResponse()));
                break;
            case COMMITRESPONSE:
                setResponse(new Response<>(msg.getCommitResponse()));
                break;
        }
    }
}

