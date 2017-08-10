import org.szymie.client.strong.pessimistic.NettySerializableTransaction;
import org.szymie.client.strong.pessimistic.Transaction;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Semaphore;

public class Test2 {

    public static void main(String[] args) {

        Semaphore t2Sem = new Semaphore(0);
        Semaphore t3Sem = new Semaphore(0);

        Thread t1 = new Thread(() -> {

            NettySerializableTransaction t = new NettySerializableTransaction();

            Map<String, Integer> reads = new HashMap<String, Integer>() {{ put("a", 1); }};
            Map<String, Integer> writes = new HashMap<String, Integer>() {{ put("b", 1); }};

            t.begin(reads, writes);

            t2Sem.release(1);

            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }

            int a = Integer.parseInt(t.read("a") == null ? "0" : t.read("a"));
            System.err.println("t1, a: " + a);
            t.write("b", String.valueOf(a + 1));

            System.err.println("t1: " + t.commit());
        });

        Thread t2 = new Thread(() -> {

            NettySerializableTransaction t = new NettySerializableTransaction();

            Map<String, Integer> reads = new HashMap<String, Integer>() {{ put("b", 1); }};
            Map<String, Integer> writes = new HashMap<String, Integer>() {{ put("a", 1); }};

            try {
                t2Sem.acquire(1);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }

            t.begin(reads, writes);

            t3Sem.release(1);

            try {
                Thread.sleep(1100);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }

            int b = Integer.parseInt(t.read("b") == null ? "0" : t.read("b"));
            System.err.println("t2, b: " + b);
            t.write("a", String.valueOf(b + 1));

            System.err.println("t2: " + t.commit());
        });

        Thread t3 = new Thread(() -> {

            NettySerializableTransaction t = new NettySerializableTransaction();

            Map<String, Integer> reads = new HashMap<String, Integer>() {{ put("a", 1); }};
            Map<String, Integer> writes = new HashMap<String, Integer>() {{ put("b", 1); }};

            try {
                t3Sem.acquire(1);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }

            t.begin(reads, writes);

            try {
                Thread.sleep(500);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }

            int a = Integer.parseInt(t.read("a") == null ? "0" : t.read("a"));
            System.err.println("t3, a: " + a);
            t.write("b", String.valueOf(0));

            System.err.println("t3: " + t.commit());
        });

        t1.start();
        t2.start();
        t3.start();

        try {
            t1.join();
            t2.join();
            t3.join();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        Transaction serializableTransaction = new NettySerializableTransaction();

        Map<String, Integer> reads = new HashMap<String, Integer>() {{ put("a", 1); put("b", 1); }};
        Map<String, Integer> writes = new HashMap<>();

        serializableTransaction.begin(reads, writes);

        String a = serializableTransaction.read("a");
        System.err.println("a: " + a);
        String b = serializableTransaction.read("b");
        System.err.println("b: " + b);
        serializableTransaction.commit();
    }

}
