package org.szymie;

import org.szymie.client.strong.optimistic.Transaction;
import org.szymie.client.strong.optimistic.TransactionFactory;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;

public class Benchmark {

    public class Operations {
        public static final int READ = 1;
        public static final int WRITE = 2;
    }

    public enum SaturationLevel {
        LOW(1), AVERAGE(5), HIGH(9);

        public final int value;

        SaturationLevel(int value) {
            this.value = value;
        }
    }

    private TransactionFactory factory;
    private List<String> keys;

    private int numberOfReadsInQuery;
    private int numberOfReadsInUpdate;
    private int numberOfWritesInUpdate;
    private long delayInMillis;

    private Random random;

    private boolean stop;
    private AtomicLong counter;

    private CyclicBarrier barrier;

    public Benchmark(TransactionFactory factory, int numberOfKeys, int numberOfReadsInQuery, int numberOfReadsInUpdate, int numberOfWritesInUpdate, long delayInMillis) {

        this.factory = factory;

        keys = new ArrayList<>(numberOfKeys);

        for(int i = 0; i < numberOfKeys; i++) {
            keys.add(i, "key" + i);
        }

        this.numberOfReadsInQuery = numberOfReadsInQuery;
        this.numberOfReadsInUpdate = numberOfReadsInUpdate;
        this.numberOfWritesInUpdate = numberOfWritesInUpdate;
        this.delayInMillis = delayInMillis;

        random = new Random();

        stop = false;
        counter = new AtomicLong(0);

    }

    public void execute(SaturationLevel updateSaturationLevel, int numberOfThreads) {

        //barrier = new CyclicBarrier(numberOfThreads + 1);

        BlockingQueue<Runnable> blockingQueue = new LinkedBlockingQueue<>(numberOfThreads);
        ExecutorService executor = new ThreadPoolExecutor(numberOfThreads, numberOfThreads, 5000, TimeUnit.MILLISECONDS, blockingQueue);

        //long startTime = System.nanoTime();

        for(int i = 0; ; i = (i + 1) % 10) {

            Runnable runnable;

            if(i < updateSaturationLevel.value) {
                runnable = () -> executeTransaction(numberOfReadsInUpdate, numberOfWritesInUpdate);
            } else {
                runnable = () -> executeTransaction(numberOfReadsInQuery, 0);
            }

            executor.submit(runnable);
        }

        //double elapsedTime = TimeUnit.NANOSECONDS.toSeconds(System.nanoTime() - startTime);

        //System.err.println("result: " + counter.get() / elapsedTime);
    }

    private void executeTransaction(int numberOfReads, int numberOfWrites) {

        Transaction transaction = factory.newSerializableTransaction();

        Map<String, Integer> operations = generateOperations(numberOfReads, numberOfWrites);

        //while(!stop) {

            boolean commit;

            do {
                transaction.begin();

                operations.forEach((key, operation) -> {

                    if((operation & Operations.READ) != 0) {
                        transaction.read(key);
                    }

                    if((operation & Operations.WRITE) != 0) {
                        transaction.write(key, String.valueOf(random.nextInt()));
                    }
                });

                if(delayInMillis != 0) {
                    try {
                        Thread.sleep(delayInMillis);
                    } catch (InterruptedException ignore) { }
                }

                commit = transaction.commit();

            } while (!commit);

            counter.incrementAndGet();
        //}
    }

    private Map<String, Integer> generateOperations(int numberOfReads, int numberOfWrites) {

        Map<String, Integer> operations = new HashMap<>();

        for(int i = 0; i < numberOfReads; ) {
            String key = keys.get(random.nextInt(keys.size()));
            if(operations.put(key, Operations.READ) == null) {
                i++;
            }
        }

        for(int i = 0; i < numberOfWrites; ) {
            String key = keys.get(random.nextInt(keys.size()));
            if(operations.put(key, operations.getOrDefault(key, 0) | Operations.WRITE) == null) {
                i++;
            }
        }

        return operations;
    }

    public void stop() {
        stop = true;
    }
}
