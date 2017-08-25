package org.szymie.benchmark;

import org.apache.jmeter.config.Arguments;
import org.apache.jmeter.protocol.java.sampler.AbstractJavaSamplerClient;
import org.apache.jmeter.protocol.java.sampler.JavaSamplerContext;
import org.apache.jmeter.samplers.SampleResult;
import org.szymie.Benchmark;
import org.szymie.Configuration;
import org.szymie.client.strong.ReadWriteRemoveCommitTransaction;
import sun.reflect.generics.reflectiveObjects.NotImplementedException;

import java.util.*;
import java.util.concurrent.ThreadLocalRandom;

public abstract class BaseROJMeterRequest extends AbstractJavaSamplerClient implements Sleep {

    List<String> keys;
    Random random;
    long delayInMillis;
    int randomDelayInMillis;
    int numberOfKeys;
    int numberOfClientThreads;
    Map<String, Integer> reads;
    Map<String, Integer> operations;
    Configuration configuration;

    @Override
    public Arguments getDefaultParameters() {

        Arguments params = new Arguments();

        params.addArgument("replicas", "${replicas}");
        params.addArgument("paxosProcesses", "${paxosProcesses}");

        params.addArgument("numberOfReadsInQuery", "${numberOfReadsInQuery}");
        params.addArgument("delayInMillis", "${delayInMillis}");
        params.addArgument("randomDelayInMillis", "${randomDelayInMillis}");
        params.addArgument("numberOfKeys", "${numberOfKeys}");
        params.addArgument("numberOfClientThreads", "${numberOfClientThreads}");

        return params;
    }

    @Override
    public void setupTest(JavaSamplerContext context) {

        super.setupTest(context);

        int numberOfReadsInQuery = context.getIntParameter("numberOfReadsInQuery");
        delayInMillis = context.getLongParameter("delayInMillis", 0);
        randomDelayInMillis = context.getIntParameter("randomDelayInMillis", 0);
        numberOfKeys = context.getIntParameter("numberOfKeys");
        numberOfClientThreads = context.getIntParameter("numberOfClientThreads", 0);

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
        operations = generateOperations(numberOfReadsInQuery);
    }

    private Map<String, Integer> generateOperations(int numberOfReadsInQuery) {

        Map<String, Integer> operations = new HashMap<>();

        int[] readKeyIds = ThreadLocalRandom.current().ints(0, numberOfKeys).distinct().limit(numberOfReadsInQuery).toArray();

        for(int keyId : readKeyIds) {
            String key = "key" + keyId;
            operations.put(key, Benchmark.Operations.READ);
            reads.put(key, 1);
        }

        return operations;
    }

    void executeOperations(ReadWriteRemoveCommitTransaction transaction) {

        for (Map.Entry<String, Integer> read : reads.entrySet()) {
            transaction.read(read.getKey());
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
