import org.szymie.client.strong.optimistic.NettySerializableTransaction;

import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class Read {

    public static void main(String[] args) {

        NettySerializableTransaction transaction = new NettySerializableTransaction();

        transaction.begin();

        int[] ints = IntStream.range(0, 300).toArray();

        int empties = 0;

        for(int i : ints) {

            String value = transaction.read("key" + i);

            if(value.isEmpty()) {
                empties++;
            }
        }

        transaction.commit();

        System.err.println("empties: " + empties);
    }
}
