import org.szymie.Configuration;
import org.szymie.client.strong.causal.NettyCausalTransaction;

        import org.szymie.client.strong.causal.NettyCausalTransaction;
        import org.szymie.client.strong.sequential.NettySequentialTransaction;
        import java.util.HashMap;
        import java.util.List;
        import java.util.Map;
        import java.util.concurrent.CountDownLatch;

public class  CausalTest2 {

    public static void main(String[] args) {

        Map<String, String> properties = new HashMap<>();

        properties.put("replicas", "0-127.0.0.1:8081");

        Configuration configuration = new Configuration(properties);

        Map<String, String> properties2 = new HashMap<>();

        properties2.put("replicas", "0-127.0.0.1:8080");

        Configuration configuration2 = new Configuration(properties2);

        NettyCausalTransaction transaction2 = new NettyCausalTransaction(configuration2);

        transaction2.begin();

        List<String> aValues = transaction2.read("a");

        System.out.println("aValues: " + String.join(", ", aValues));

        transaction2.write("a", "val1");

        transaction2.commit();
    }
}
