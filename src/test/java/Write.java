import org.szymie.Configuration;
import org.szymie.client.strong.optimistic.NettySerializableTransaction;

import java.util.HashMap;
import java.util.stream.IntStream;

public class Write {

    public static void main(String[] args) {

        HashMap<String, String> properties = new HashMap<>();
        properties.put("paxosProcesses", "0:127.0.0.1:2000:3000,1:127.0.0.1:2001:3001,2:127.0.0.1:2002:3002");
        properties.put("replicas", "0-127.0.0.1:8080");

        NettySerializableTransaction t = new NettySerializableTransaction(new Configuration(properties));

        t.begin();

        IntStream.range(0, 300).forEach(i -> t.write("key" + i, "val"));

        t.commit();
    }

}
