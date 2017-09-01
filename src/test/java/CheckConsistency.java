import org.szymie.Configuration;
import org.szymie.client.strong.pessimistic.NettySerializableTransaction;

import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class CheckConsistency {


    public static void main(String[] args) {

        List<String> replicas = Arrays.asList("0-127.0.0.1:8080", "1-127.0.0.1:8081", "2-127.0.0.1:8082");

        List<NettySerializableTransaction> transactions = replicas.stream().map(replica ->
            new NettySerializableTransaction(createConfiguration(replica))
        ).collect(Collectors.toList());

        List<String> keys = IntStream.range(0, 300).mapToObj(index -> "key" + index).collect(Collectors.toList());

        HashMap<String, Integer> reads = new HashMap<>();
        HashMap<String, Integer> writes = new HashMap<>();

        for(String key : keys) {
            reads.put(key, 1);
        }


        Map<String, List<String>> results = new HashMap<>();

        for(NettySerializableTransaction transaction : transactions) {

            transaction.begin(reads, writes);

            keys.forEach(key -> {
                String value = transaction.read(key);
                List<String> result = results.computeIfAbsent(key, ignore -> new LinkedList<>());
                result.add(value);
            });

            transaction.commit();
        }

        Set<String> wrongKeys = new HashSet<>();

        results.forEach((key, result) -> {
            if(!(result.get(0).equals(result.get(1)) && result.get(1).equals(result.get(2)) && result.get(0).equals(result.get(2)))) {
                wrongKeys.add(key);
            }
        });

        System.out.println("wrong keys: " + String.join(", ", wrongKeys));
    }

    private static Configuration createConfiguration(String replicas) {
        Map<String, String> properties = new HashMap<>();
        properties.put("replicas", replicas);
        return new Configuration(properties);
    }

}
