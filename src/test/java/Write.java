import org.szymie.client.strong.pessimistic.NettySerializableTransaction;

import java.util.HashMap;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class Write {

    public static void main(String[] args) {

        NettySerializableTransaction transaction = new NettySerializableTransaction();

        HashMap<String, Integer> writes = new HashMap<>();

        writes.put("key99", 2);

        transaction.begin(new HashMap<>(), writes);

        transaction.write("key99", "e");

        transaction.commit();
    }
}



