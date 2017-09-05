import org.szymie.Configuration;
import org.szymie.client.strong.causal.NettyCausalTransaction;
import org.szymie.client.strong.sequential.NettySequentialTransaction;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;

public class  CausalTest {

    public static void main(String[] args) {

        Map<String, String> properties = new HashMap<>();

        properties.put("replicas", "0-127.0.0.1:8081");

        Configuration configuration = new Configuration(properties);



        CountDownLatch latch = new CountDownLatch(1);

        Thread t0 = new Thread(() -> {

            Map<String, String> properties1 = new HashMap<>();

            properties1.put("replicas", "0-127.0.0.1:8080");

            Configuration configuration1 = new Configuration(properties1);

            NettyCausalTransaction transaction1 = new NettyCausalTransaction(configuration1);

            transaction1.begin();

            List<String> aValues = transaction1.read("a");

            try {
                latch.await();
            } catch (InterruptedException e) {
            }

            System.out.println("aValues: " + String.join(", ", aValues));

            transaction1.write("a", "val0");

            transaction1.commit();
        });

        Thread t1 = new Thread(() -> {

            Map<String, String> properties2 = new HashMap<>();

            properties2.put("replicas", "0-127.0.0.1:8080");

            Configuration configuration2 = new Configuration(properties2);

            NettyCausalTransaction transaction2 = new NettyCausalTransaction(configuration2);

            transaction2.begin();

            List<String> aValues = transaction2.read("a");

            latch.countDown();

            System.out.println("aValues: " + String.join(", ", aValues));

            transaction2.write("a", "val1");

            transaction2.commit();
        });

        //t0.start();
        //t1.start();

        try {
            t0.join();
            t1.join();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        NettyCausalTransaction transaction = new NettyCausalTransaction(configuration);

        transaction.begin();

        List<String> aValues = transaction.read("a");

        System.err.println("aValues: " + String.join(", ", aValues));

        transaction.commit();
    }
}
