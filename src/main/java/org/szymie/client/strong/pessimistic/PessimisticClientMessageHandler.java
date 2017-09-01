package org.szymie.client.strong.pessimistic;

import io.netty.channel.ChannelHandlerContext;
import org.szymie.client.strong.optimistic.BaseClientMessageHandler;
import org.szymie.messages.Messages;

class PessimisticClientMessageHandler extends BaseClientMessageHandler {

    @Override
    public void channelRead0(ChannelHandlerContext ctx, Messages.Message msg) throws Exception {

        System.err.println("msg " + msg);

        switch(msg.getOneofMessagesCase()) {
            case BEGINTRANSACTIONRESPONSE:
                setResponse(new Response<>(msg.getBeginTransactionResponse()));
                break;
            case COMMITRESPONSE:
                setResponse(new Response<>(msg.getCommitResponse()));
                break;
            case READRESPONSE:
                setResponse(new Response<>(msg.getReadResponse()));
                break;
            case INITRESPONSE:
                setResponse(new Response<>(msg.getInitResponse()));
        }
    }
}