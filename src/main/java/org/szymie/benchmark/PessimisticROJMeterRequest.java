package org.szymie.benchmark;

import org.apache.jmeter.config.Arguments;
import org.apache.jmeter.protocol.java.sampler.JavaSamplerContext;
import org.apache.jmeter.samplers.SampleResult;
import org.szymie.client.strong.pessimistic.NettySerializableTransaction;

import java.util.HashMap;

public class PessimisticROJMeterRequest extends BaseROJMeterRequest {

    @Override
    public SampleResult runTest(JavaSamplerContext javaSamplerContext) {

        System.err.println("start");

        SampleResult result = new SampleResult();

        result.sampleStart();

        NettySerializableTransaction transaction = new NettySerializableTransaction(numberOfClientThreads, configuration);

        boolean commit;

        int attempts = 0;

        do {
            attempts++;

            transaction.begin(reads, new HashMap<>());

            System.err.println("executeOperations");

            executeOperations(transaction);

            commit = transaction.commit();

        } while (!commit);

        System.err.println("committed");

        result.sampleEnd();
        result.setSuccessful(true);

        if(attempts > 1) {
            result.setErrorCount(1);
            result.setSuccessful(false);
        }

        return result;
    }
}
