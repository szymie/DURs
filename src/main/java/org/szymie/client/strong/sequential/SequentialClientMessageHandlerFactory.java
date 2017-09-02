package org.szymie.client.strong.sequential;

import org.szymie.client.strong.ClientMessageHandlerFactory;
import org.szymie.client.strong.optimistic.BaseClientMessageHandler;

public class SequentialClientMessageHandlerFactory implements ClientMessageHandlerFactory {

    @Override
    public SequentialClientMessageHandler create() {
        return new SequentialClientMessageHandler();
    }
}
