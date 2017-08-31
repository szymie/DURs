import org.jgroups.JChannel;
import org.jgroups.Message;
import org.szymie.messages.StateUpdate;
import org.szymie.server.strong.pessimistic.GroupMessenger;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.concurrent.Callable;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class Test {

    public static void main(String[] args) throws Exception {

        JChannel channel = new JChannel();
        channel.connect("cluster-0");

        ArrayList<Callable<String>> threads = new ArrayList<>();

        for (int i = 0; i < 30; i++) {

            final long index = i;

            Callable<String> t = () -> {

                Message message = new Message(null, new StateUpdate(index, 0, new HashMap<>()));
                try {
                    channel.send(message);
                } catch (Exception e) {
                    e.printStackTrace();
                }

                return "";
            };

            threads.add(t);

            System.err.println("sent");
        }


        ExecutorService executorService = Executors.newFixedThreadPool(40);

        executorService.invokeAll(threads);

    }

}
