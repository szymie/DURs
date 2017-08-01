import org.szymie.client.strong.pessimistic.SerializableTransaction;

import java.util.HashMap;
import java.util.Map;

public class ReadB {

    public static void main(String[] args) {

        SerializableTransaction t = new SerializableTransaction();


        Map<String, Integer> reads = new HashMap<String, Integer>() {{ put("b", 1); }};
        Map<String, Integer> writes = new HashMap<>();

        t.begin(reads, writes);

        System.err.println(t.read("b"));

        t.commit();
    }

}
