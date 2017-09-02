package org.szymie.client.strong.sequential;

import io.netty.channel.ChannelHandlerContext;
import org.szymie.client.strong.optimistic.BaseClientMessageHandler;
import org.szymie.messages.Messages;

class SequentialClientMessageHandler extends BaseClientMessageHandler {

    @Override
    public void channelRead0(ChannelHandlerContext ctx, Messages.Message msg) throws Exception {

        System.err.println("msg " + msg);

        switch(msg.getOneofMessagesCase()) {
            case TRANSACTIONEXECUTIONRESPONSE:
                setResponse(new Response<>(msg.getTransactionExecutionResponse()));
                break;
        }
    }
}