package org.szymie.server.strong.pessimistic;

import org.jgroups.JChannel;
import org.jgroups.Message;
import org.jgroups.Receiver;

public class GroupMessenger {

    private JChannel channel;

    public GroupMessenger(String groupName, Receiver messageReceiver) {

        try {

            channel = new JChannel();

            if(messageReceiver != null) {
                channel.setReceiver(messageReceiver);
            }

            channel.connect(groupName);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public <T> void send(T object) {

        try {
            Message message = new Message(null, object);
            channel.send(message);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
