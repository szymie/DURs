package org.szymie.benchmark;

import org.apache.jmeter.protocol.java.sampler.AbstractJavaSamplerClient;
import org.apache.jmeter.protocol.java.sampler.JavaSamplerContext;
import org.apache.jmeter.samplers.SampleResult;
import org.apache.jmeter.testelement.property.JMeterProperty;
import org.apache.jmeter.threads.JMeterContext;
import org.apache.jmeter.threads.JMeterContextService;
import org.szymie.Benchmark;
import org.szymie.Configuration;
import org.szymie.client.strong.ReadWriteRemoveCommitTransaction;
import org.szymie.client.strong.pessimistic.NettySerializableTransaction;

import java.util.*;

public abstract class BaseJMeterRequest extends AbstractJavaSamplerClient {

    private List<String> keys;
    Random random;
    long delayInMillis;
    Map<String, Integer> operations;
    Map<String, Integer> reads;
    Map<String, Integer> writes;
    Configuration configuration;

    @Override
    public void setupTest(JavaSamplerContext context) {

        super.setupTest(context);

        int numberOfReadsInQuery = context.getIntParameter("numberOfReadsInQuery");
        int numberOfReads = context.getIntParameter("numberOfReads");
        int numberOfWrites = context.getIntParameter("numberOfWrites");
        delayInMillis = context.getIntParameter("delayInMillis");

        int numberOfKeys = context.getIntParameter("numberOfKeys");

        String replicas = context.getParameter("replicas");
        String paxosProcesses = context.getParameter("paxosProcesses");

        Map<String, String> properties = new HashMap<>();

        if(replicas != null) {
            System.err.println("replicas: " + replicas);
            properties.put("replicas", replicas);
        } else {
            System.err.println("null");

            JMeterProperty replicas1 = JMeterContextService.getContext().getCurrentSampler().getProperty("replicas");
            JMeterProperty replicas2 = JMeterContextService.getContext().getCurrentSampler().getProperty("${replicas}");

            System.err.println("replicas1: " + replicas1);
            System.err.println("replicas2: " + replicas2);

        }

        if(paxosProcesses != null) {
            System.err.println("paxosProcesses: " + paxosProcesses);
            properties.put("paxosProcesses", paxosProcesses);
        } else {
            System.err.println("null");
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

        for(int i = 0; i < numberOfReads; ) {
            String key = keys.get(random.nextInt(keys.size()));
            if(operations.put(key, Benchmark.Operations.READ) == null) {
                i++;
                reads.put(key, reads.getOrDefault(key, 0) + 1);
            }
        }

        for(int i = 0; i < numberOfWrites; ) {
            String key = keys.get(random.nextInt(keys.size()));
            if(operations.put(key, operations.getOrDefault(key, 0) | Benchmark.Operations.WRITE) == null) {
                i++;
                writes.put(key, reads.getOrDefault(key, 0) + 1);
            }
        }

        return operations;
    }

    void executeOperations(ReadWriteRemoveCommitTransaction transaction) {

        //NettySerializableTransaction t = (NettySerializableTransaction) transaction;

        //int i = 0;

        for (Map.Entry<String, Integer> operationAndKey : operations.entrySet()) {

            String key = operationAndKey.getKey();
            int operation = operationAndKey.getValue();

            if((operation & Benchmark.Operations.READ) != 0) {
                //System.err.println(t.getTimestamp() + " trying to read " + i);
                transaction.read(key);
                //System.err.println(t.getTimestamp() + " read " + i);
                //i++;
            }

            if((operation & Benchmark.Operations.WRITE) != 0) {
                transaction.write(key, String.valueOf(random.nextInt()));
            }
        }
    }
}
