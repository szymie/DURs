package org.szymie.benchmark;

import org.apache.jmeter.protocol.java.sampler.AbstractJavaSamplerClient;
import org.apache.jmeter.protocol.java.sampler.JavaSamplerContext;
import org.apache.jmeter.samplers.SampleResult;
import org.szymie.Benchmark;
import org.szymie.client.strong.optimistic.NettySerializableTransaction;

import java.util.*;

public class BaseJMeterRequest extends AbstractJavaSamplerClient {

    private List<String> keys;
    private Random random;
    private long delayInMillis;
    private Map<String, Integer> operations;

    @Override
    public void setupTest(JavaSamplerContext context) {

        super.setupTest(context);

        int numberOfReadsInQuery = context.getIntParameter("numberOfReadsInQuery");
        int numberOfReads = context.getIntParameter("numberOfReads");
        int numberOfWrites = context.getIntParameter("numberOfWrites");
        delayInMillis = context.getIntParameter("delayInMillis");

        int numberOfKeys = context.getIntParameter("numberOfKeys");

        keys = new ArrayList<>(numberOfKeys);

        for(int i = 0; i < numberOfKeys; i++) {
            keys.add(i, "key" + i);
        }

        random = new Random(Thread.currentThread().getId());
        operations = generateOperations(numberOfReads, numberOfWrites);
    }

    @Override
    public SampleResult runTest(JavaSamplerContext javaSamplerContext) {


        SampleResult result = new SampleResult();

        result.sampleStart();

        NettySerializableTransaction transaction = new NettySerializableTransaction();

        boolean commit;

        do {
            transaction.begin();

            operations.forEach((key, operation) -> {

                if((operation & Benchmark.Operations.READ) != 0) {
                    transaction.read(key);
                }

                if((operation & Benchmark.Operations.WRITE) != 0) {
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

        result.sampleEnd();
        result.setSuccessful(true);

        return result;
    }

    private Map<String, Integer> generateOperations(int numberOfReads, int numberOfWrites) {

        Map<String, Integer> operations = new HashMap<>();

        for(int i = 0; i < numberOfReads; ) {
            String key = keys.get(random.nextInt(keys.size()));
            if(operations.put(key, Benchmark.Operations.READ) == null) {
                i++;
            }
        }

        for(int i = 0; i < numberOfWrites; ) {
            String key = keys.get(random.nextInt(keys.size()));
            if(operations.put(key, operations.getOrDefault(key, 0) | Benchmark.Operations.WRITE) == null) {
                i++;
            }
        }

        return operations;
    }
}
