package org.szymie.client.strong.pessimistic;

import org.szymie.client.strong.ClientMessageHandlerFactory;
import org.szymie.client.strong.optimistic.BaseClientMessageHandler;

public class PessimisticClientMessageHandlerFactory implements ClientMessageHandlerFactory {

    @Override
    public PessimisticClientMessageHandler create() {
        return new PessimisticClientMessageHandler();
    }
}
