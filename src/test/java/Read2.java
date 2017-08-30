
import org.szymie.client.strong.pessimistic.NettySerializableTransaction;

import java.io.IOException;
import java.util.HashMap;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class Read2 {

    public static void main(String[] args) throws IOException {

        NettySerializableTransaction transaction = new NettySerializableTransaction();

        HashMap<String, Integer> reads = new HashMap<>();

        reads.put("key99", 1);
        reads.put("key262", 1);

        transaction.begin(reads, new HashMap<>());

        String r1 = transaction.read("key99");
        String r2 = transaction.read("key262");

        System.err.println("r1: " + r1);
        System.err.println("r2: " + r2);

        System.in.read();

        transaction.commit();
    }
}
