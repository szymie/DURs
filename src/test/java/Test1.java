import org.szymie.client.strong.pessimistic.NettySerializableTransaction;
import org.szymie.client.strong.pessimistic.Transaction;

import java.util.HashMap;

public class Test1 {

    public static void main(String[] args) {

        Transaction transaction = new NettySerializableTransaction();

        HashMap<String, Integer> reads = new HashMap<String, Integer>() {{ put("a", 1); }};
        HashMap<String, Integer> writes = new HashMap<String, Integer>() {{ put("a", 1); }};

        transaction.begin(reads, writes);

        String aString = transaction.read("a");

        int a = Integer.parseInt(aString.isEmpty() ? "0" : aString);

        System.err.println(a);

        transaction.write("a", String.valueOf(a + 1));

        System.err.println(transaction.commit());
    }
}
