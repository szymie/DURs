import org.szymie.client.strong.pessimistic.NettySerializableTransaction;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;

public class Test4 {

    public static void main(String[] args) throws InterruptedException {

        ExecutorService executor = Executors.newFixedThreadPool(10);

        List<Callable<Integer>> callableList = new ArrayList<>(10);

        for(int i = 0; i < 10; i++) {

            final int index = i;

            callableList.add(() -> {

                NettySerializableTransaction t = new NettySerializableTransaction();

                Map<String, Integer> reads = new HashMap<>();
                Map<String, Integer> writes = new HashMap<String, Integer>() {{ put("b", 1); }};

                t.begin(reads, writes);
                t.write("b", String.valueOf(index));
                t.commit();

                return index;
            });
        }

        List<Future<Integer>> futures = executor.invokeAll(callableList);

        futures.forEach(integerFuture -> {
            try {
                System.out.println(integerFuture.get());
            } catch (InterruptedException | ExecutionException e) {
                e.printStackTrace();
            }
        });
    }

}
