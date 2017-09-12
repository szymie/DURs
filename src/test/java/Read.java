import org.szymie.Configuration;
import org.szymie.client.strong.optimistic.NettySerializableTransaction;

import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class Read {

    public static void main(String[] args) {

        Map<String, String> properties = new HashMap<>();

        properties.put("replicas", "0-127.0.0.1:8080");

        Configuration configuration = new Configuration(properties);

        NettySerializableTransaction transaction = new NettySerializableTransaction(configuration);

        transaction.begin();

        int[] ints = IntStream.range(0, 1000).toArray();

        int empties = 0;

        HashMap<String, String> summary = new HashMap<>();

        for(int i : ints) {

            String value = transaction.read("key" + i);

            if(!value.isEmpty()) {
                summary.put("key" + i, value);
                empties++;
            }
        }

        transaction.commit();

        summary.forEach((key, value) -> System.err.println(key + ": " + value));

        System.err.println("empties: " + empties);
    }
}
