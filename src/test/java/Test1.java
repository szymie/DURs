import org.szymie.client.strong.pessimistic.SerializableTransaction;
import org.szymie.client.strong.pessimistic.Transaction;

import java.util.HashMap;
import java.util.HashSet;

public class Test1 {

    public static void main(String[] args) {

        Transaction transaction = new SerializableTransaction();

        HashMap<String, Integer> reads = new HashMap<String, Integer>() {{ put("a", 1); }};
        HashMap<String, Integer> writes = new HashMap<String, Integer>() {{ put("a", 1); }};

        transaction.begin(reads, writes);

        String aString = transaction.read("a");

        int a = Integer.parseInt(aString == null ? "0" : aString);

        System.err.println(a);

        transaction.write("a", String.valueOf(a + 1));

        System.err.println(transaction.commit());
    }
}
