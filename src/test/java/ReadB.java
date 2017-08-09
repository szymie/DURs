import org.szymie.client.strong.optimistic.NettySerializableTransaction;
import org.szymie.client.strong.pessimistic.WebSocketSerializableTransaction;

import java.util.HashMap;
import java.util.Map;

public class ReadB {

    public static void main(String[] args) {

        NettySerializableTransaction t = new NettySerializableTransaction();

        t.begin();
        String a = t.read("a");
        System.err.println("a: " + a);
        t.commit();
    }

}
