import akka.actor.ActorSystem;
import lsr.paxos.client.ReplicationException;
import org.szymie.client.strong.optimistic.Transaction;

import java.io.IOException;

public class Test {

    public static void main(String[] args) throws IOException, ReplicationException, ClassNotFoundException {
        //SerializableClient client = new SerializableClient(new lsr.common.Configuration("src/main/resources/paxos.properties"));
        //client.execute(new CertificationRequest(new HashMap<>(), new HashMap<>(), 0));

        /*ActorSystem actorSystem = ActorSystem.create();

        Transaction t = new NettySerializableTransaction();


        for(int i = 0; i < 100; i++) {

            Thread thread = new Thread(() -> {

                while (true) {

                    t.begin();

                    String aString = t.read("a");

                    int a = Integer.parseInt(aString == null ? "0" : aString);
                    System.err.println(a);
                    t.write("a", String.valueOf(a + 1));

                    System.err.println(t.commit());
                }
            });

            thread.start();

            try {
                thread.join();
            } catch (InterruptedException e) {
            }
        }*/

    }

}
