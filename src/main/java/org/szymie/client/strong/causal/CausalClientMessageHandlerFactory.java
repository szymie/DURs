package org.szymie.client.strong.causal;

import org.szymie.client.strong.ClientMessageHandlerFactory;

public class CausalClientMessageHandlerFactory implements ClientMessageHandlerFactory {

    @Override
    public CausalClientMessageHandler create() {
        return new CausalClientMessageHandler();
    }
}

