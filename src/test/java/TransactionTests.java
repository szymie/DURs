
import akka.actor.ActorSystem;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;
import org.szymie.client.strong.optimistic.WebSocketSerializableTransaction;
import static org.junit.Assert.*;

@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(classes=Context.class)
public class TransactionTests {

    @Autowired
    private ActorSystem actorSystem;

    @Test
    public void test0() {

        Thread t2 = new Thread(() -> {

            WebSocketSerializableTransaction t = new WebSocketSerializableTransaction();

            t.begin();

            t.write("a", "a");

            assertTrue(t.commit());
        });

        t2.start();

        try {
            t2.join();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        Thread t0 = new Thread(() -> {

            WebSocketSerializableTransaction t = new WebSocketSerializableTransaction();

            t.begin();

            t.read("a");

            try {
                Thread.sleep(7000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }

            t.write("b", "val");

            assertFalse(t.commit());
        });

        Thread t1 = new Thread(() -> {

            WebSocketSerializableTransaction t = new WebSocketSerializableTransaction();

            t.begin();

            t.read("b");

            try {
                Thread.sleep(2000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }

            t.remove("a");

            assertTrue(t.commit());
        });

        t0.start();
        t1.start();

        try {
            t0.join();
            t1.join();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }


    @Test
    public void test1() {

        Thread t0 = new Thread(() -> {

            WebSocketSerializableTransaction t = new WebSocketSerializableTransaction();

            t.begin();

            t.read("a");

            try {
                Thread.sleep(2000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }

            t.write("b", "val");

            assertTrue(t.commit());
        });

        Thread t1 = new Thread(() -> {

            WebSocketSerializableTransaction t = new WebSocketSerializableTransaction();

            t.begin();

            t.read("b");

            try {
                Thread.sleep(4000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }

            t.write("a", "val");

            assertFalse(t.commit());
        });

        t0.start();
        t1.start();

        try {
            t0.join();
            t1.join();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
}
