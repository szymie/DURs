import org.szymie.client.strong.pessimistic.WebSocketSerializableTransaction;

import java.util.HashMap;
import java.util.Map;

public class ReadB {

    public static void main(String[] args) {

        WebSocketSerializableTransaction t = new WebSocketSerializableTransaction();


        Map<String, Integer> reads = new HashMap<String, Integer>() {{ put("b", 1); }};
        Map<String, Integer> writes = new HashMap<>();

        t.begin(reads, writes);

        System.err.println(t.read("b"));

        t.commit();
    }

}
