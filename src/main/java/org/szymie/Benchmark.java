package org.szymie;

import org.szymie.client.Transaction;
import org.szymie.client.TransactionFactory;

import java.util.*;
import java.util.concurrent.*;

public class Benchmark {

    private class Operations {
        static final int READ = 1;
        static final int WRITE = 2;
    }

    public enum SaturationLevel {
        LOW(1), AVERAGE(5), HIGH(9);

        private final int value;

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
    }

    public void execute(SaturationLevel updateSaturationSaturationLevel, int numberOfThreads) {

        BlockingQueue<Runnable> blockingQueue = new LinkedBlockingQueue<>(numberOfThreads);
        ExecutorService executor = new ThreadPoolExecutor(numberOfThreads, numberOfThreads, 50, TimeUnit.MILLISECONDS, blockingQueue);

        for(int i = 0; ; i = (i + 1) % 10) {

            Runnable runnable;

            if(i < updateSaturationSaturationLevel.value) {
                runnable = () -> executeTransaction(numberOfReadsInUpdate, numberOfWritesInUpdate);
            } else {
                runnable = () -> executeTransaction(numberOfReadsInQuery, 0);
            }

            executor.submit(runnable);
        }
    }

    private void executeTransaction(int numberOfReads, int numberOfWrites) {

        Transaction transaction = factory.newSerializableTransacion();

        Map<String, Integer> operations = generateOperations(numberOfReads, numberOfWrites);

        while(true) {

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
        }
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
}
