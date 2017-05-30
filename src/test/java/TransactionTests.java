
import akka.actor.ActorSystem;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.szymie.client.SerializableTransaction;
import static org.junit.Assert.*;

public class TransactionTests {

    private ActorSystem actorSystem;

    @Test
    public void test0() {

        Thread t2 = new Thread(() -> {

            SerializableTransaction t = new SerializableTransaction(actorSystem);

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

            SerializableTransaction t = new SerializableTransaction(actorSystem);

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

            SerializableTransaction t = new SerializableTransaction(actorSystem);

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

        ActorSystem actorSystem = ActorSystem.create();

        Thread t0 = new Thread(() -> {

            SerializableTransaction t = new SerializableTransaction(actorSystem);

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

            SerializableTransaction t = new SerializableTransaction(actorSystem);

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
