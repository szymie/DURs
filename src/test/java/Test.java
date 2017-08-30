import org.jgroups.JChannel;
import org.jgroups.Message;
import org.szymie.messages.StateUpdate;
import org.szymie.server.strong.pessimistic.GroupMessenger;

import java.util.HashMap;

public class Test {

    public static void main(String[] args) throws Exception {

        JChannel channel = new JChannel();
        channel.connect("cluster-0");

        for (int i = 0; i < 10; i++) {
            Message message = new Message(null, new StateUpdate(0, 0, new HashMap<>()));
            channel.send(message);

            System.err.println("sent");
        }


    }

}
