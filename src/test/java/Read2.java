
import org.szymie.client.strong.pessimistic.NettySerializableTransaction;

import java.util.HashMap;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class Read2 {

    public static void main(String[] args) {

        NettySerializableTransaction transaction = new NettySerializableTransaction();

        HashMap<String, Integer> reads = new HashMap<>();

        reads.put("key27", 1);

        transaction.begin(reads, new HashMap<>());

        String result = transaction.read("key27");

        transaction.commit();

        System.err.println("result: " + result);
    }
}
