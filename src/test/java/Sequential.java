import org.szymie.Configuration;
import org.szymie.client.strong.sequential.NettySequentialTransaction;

import java.util.HashMap;
import java.util.Map;

public class Sequential {

    public static void main(String[] args) {

        Map<String, String> properties = new HashMap<>();

        properties.put("replicas", "0-127.0.0.1:8080");

        Configuration configuration = new Configuration(properties);

        NettySequentialTransaction transaction = new NettySequentialTransaction(configuration);

        transaction.begin();

        transaction.read("a");
        transaction.write("a", "val");

        transaction.commit();

    }
}
