package org.szymie.benchmark;


import org.apache.jmeter.config.Arguments;
import org.apache.jmeter.protocol.java.sampler.AbstractJavaSamplerClient;
import org.apache.jmeter.protocol.java.sampler.JavaSamplerContext;
import org.apache.jmeter.samplers.SampleResult;
import org.szymie.Benchmark;
import org.szymie.Configuration;
import org.szymie.client.strong.ReadWriteRemoveCommitTransaction;

import java.util.*;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.Collectors;

public abstract class BaseRWJMeterRequest extends AbstractJavaSamplerClient implements Sleep {

    List<String> keys;
    Random random;
    long delayInMillis;
    int randomDelayInMillis;
    int numberOfKeys;
    Map<String, Integer> operations;
    Map<String, Integer> reads;
    Map<String, Integer> writes;
    Configuration configuration;

    @Override
    public Arguments getDefaultParameters() {

        Arguments params = new Arguments();

        params.addArgument("replicas", "${replicas}");
        params.addArgument("paxosProcesses", "${paxosProcesses}");

        params.addArgument("numberOfReads", "${numberOfReads}");
        params.addArgument("numberOfWrites", "${numberOfWrites}");
        params.addArgument("delayInMillis", "${delayInMillis}");
        params.addArgument("numberOfKeys", "${numberOfKeys}");

        return params;
    }

    @Override
    public void setupTest(JavaSamplerContext context) {

        super.setupTest(context);

        int numberOfReads = context.getIntParameter("numberOfReads");
        int numberOfWrites = context.getIntParameter("numberOfWrites");
        delayInMillis = context.getLongParameter("delayInMillis", 0);
        randomDelayInMillis = context.getIntParameter("randomDelayInMillis", 0);
        numberOfKeys = context.getIntParameter("numberOfKeys");

        String replicas = context.getParameter("replicas");
        String paxosProcesses = context.getParameter("paxosProcesses");

        Map<String, String> properties = new HashMap<>();

        if(replicas != null) {
            System.err.println("replicas: " + replicas);
            properties.put("replicas", replicas);
        }

        if(paxosProcesses != null) {
            System.err.println("paxosProcesses: " + paxosProcesses);
            properties.put("paxosProcesses", paxosProcesses);
        }

        configuration = new Configuration(properties);

        keys = new ArrayList<>(numberOfKeys);

        for(int i = 0; i < numberOfKeys; i++) {
            keys.add(i, "key" + i);
        }

        random = new Random(Thread.currentThread().getId());
        reads = new HashMap<>();
        writes = new HashMap<>();
        operations = generateOperations(numberOfReads, numberOfWrites);
    }

    private Map<String, Integer> generateOperations(int numberOfReads, int numberOfWrites) {

        Map<String, Integer> operations = new HashMap<>();

        int[] readKeyIds = ThreadLocalRandom.current().ints(0, numberOfKeys).distinct().limit(numberOfReads).toArray();

        for(int keyId : readKeyIds) {
            String key = "key" + keyId;
            operations.put(key, Benchmark.Operations.READ);
            reads.put(key, 1);
        }

        int[] writeKeyIds = ThreadLocalRandom.current().ints(0, numberOfKeys).distinct().limit(numberOfWrites).toArray();

        for (int keyId : writeKeyIds) {
            String key = "key" + keyId;
            operations.put(key, operations.getOrDefault(key, 0) | Benchmark.Operations.WRITE);
            writes.put(key, 1);
        }

        return operations;
    }

    void executeOperations(ReadWriteRemoveCommitTransaction transaction) {

        for (Map.Entry<String, Integer> operationAndKey : operations.entrySet()) {

            String key = operationAndKey.getKey();
            int operation = operationAndKey.getValue();

            if((operation & Benchmark.Operations.READ) != 0) {
                transaction.read(key);
            }

            if((operation & Benchmark.Operations.WRITE) != 0) {
                transaction.write(key, String.valueOf(random.nextInt()));
            }
        }

        if(delayInMillis != 0) {
            sleep(delayInMillis);
        }

        if(randomDelayInMillis != 0) {
            int randomDelay = random.nextInt(randomDelayInMillis);
            sleep(randomDelay);
        }
    }
}