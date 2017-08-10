package org.szymie.client.strong.optimistic;

import org.szymie.client.strong.ClientMessageHandlerFactory;

public class OptimisticClientMessageHandlerFactory implements ClientMessageHandlerFactory {

    @Override
    public OptimisticClientMessageHandler create() {
        return new OptimisticClientMessageHandler();
    }
}
