package org.szymie.benchmark;

import com.fasterxml.jackson.databind.ser.Serializers;
import org.apache.jmeter.config.Arguments;
import org.apache.jmeter.protocol.java.sampler.JavaSamplerContext;
import org.apache.jmeter.samplers.SampleResult;
import org.szymie.Benchmark;
import org.szymie.Configuration;
import org.szymie.client.strong.optimistic.NettySerializableTransaction;

import java.util.HashMap;
import java.util.Map;

public abstract class BaseOptimisticJMeterRequest extends BaseJMeterRequest {

    @Override
    public SampleResult runTest(JavaSamplerContext javaSamplerContext) {

        SampleResult result = new SampleResult();

        result.sampleStart();

        Map<String, String> configuration = new HashMap<>();
        configuration.put("replicas", replicas);

        NettySerializableTransaction transaction = new NettySerializableTransaction(new Configuration(configuration));

        boolean commit;

        int attempts = 0;

        do {
            attempts++;

            transaction.begin();

            executeOperations(transaction);

            if(delayInMillis != 0) {
                try {
                    Thread.sleep(delayInMillis);
                } catch (InterruptedException ignore) { }
            }

            commit = transaction.commit();

        } while (!commit);

        result.sampleEnd();
        result.setSuccessful(true);
        result.setSampleCount(attempts);

        return result;
    }
}
