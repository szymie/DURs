import org.szymie.Configuration;
import org.szymie.client.strong.optimistic.NettySerializableTransaction;

import java.util.HashMap;
import java.util.stream.IntStream;

public class ReadB {

    public static void main(String[] args) {

        HashMap<String, String> properties = new HashMap<>();
        properties.put("paxosProcesses", "0:127.0.0.1:2000:3000,1:127.0.0.1:2001:3001,2:127.0.0.1:2002:3002");
        properties.put("replicas", "0-127.0.0.1:8080");

        NettySerializableTransaction t = new NettySerializableTransaction(new Configuration(properties));

        t.begin();


        long sum = IntStream.range(0, 300).mapToLong(i -> {

            String read = t.read("key" + i);

            if (read == null) {
                return 1L;
            } else {
                return 0L;
            }
        }).sum();

        System.out.println("sum: "+ sum);

        t.commit();
    }

}
