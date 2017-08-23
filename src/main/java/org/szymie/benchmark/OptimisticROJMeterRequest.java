package org.szymie.benchmark;

import org.apache.jmeter.config.Arguments;
import org.apache.jmeter.protocol.java.sampler.JavaSamplerContext;
import org.apache.jmeter.samplers.SampleResult;
import org.szymie.client.strong.optimistic.NettySerializableTransaction;

public class OptimisticROJMeterRequest extends BaseROJMeterRequest {

    @Override
    public SampleResult runTest(JavaSamplerContext javaSamplerContext) {

        System.err.println("start");

        SampleResult result = new SampleResult();

        result.sampleStart();

        NettySerializableTransaction transaction = new NettySerializableTransaction(configuration);

        boolean commit;

        int attempts = 0;

        do {
            attempts++;

            transaction.begin();

            System.err.println("executeOperations");

            executeOperations(transaction);

            commit = transaction.commit();

        } while (!commit);

        System.err.println("committed");

        result.sampleEnd();
        result.setSuccessful(true);

        return result;
    }
}