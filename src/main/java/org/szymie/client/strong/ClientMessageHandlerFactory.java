package org.szymie.client.strong;

import org.szymie.client.strong.optimistic.BaseClientMessageHandler;

public interface ClientMessageHandlerFactory {
    BaseClientMessageHandler create();
}
