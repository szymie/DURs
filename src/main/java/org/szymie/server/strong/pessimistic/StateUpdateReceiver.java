package org.szymie.server.strong.pessimistic;

import org.jgroups.Message;
import org.jgroups.ReceiverAdapter;
import org.jgroups.View;
import org.szymie.messages.StateUpdate;

import java.util.concurrent.Semaphore;
import java.util.stream.Collectors;

public class StateUpdateReceiver extends ReceiverAdapter {

    private Semaphore s = new Semaphore(1);

    @Override
    public void receive(Message message) {

        super.receive(message);

        StateUpdate stateUpdate = message.getObject();

    }

    @Override
    public void viewAccepted(View view) {
        super.viewAccepted(view);
        String members = String.join(", ", view.getMembers().stream().map(Object::toString).collect(Collectors.toList()));
        System.out.println("View update: " + members);
    }
}
