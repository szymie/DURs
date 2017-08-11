package org.szymie.benchmark;

import com.fasterxml.jackson.databind.ser.Serializers;
import org.apache.jmeter.protocol.java.sampler.JavaSamplerContext;
import org.apache.jmeter.samplers.SampleResult;
import org.szymie.Benchmark;
import org.szymie.client.strong.optimistic.NettySerializableTransaction;

public class BaseOptimisticJMeterRequest extends BaseJMeterRequest {

    @Override
    public SampleResult runTest(JavaSamplerContext javaSamplerContext) {

        SampleResult result = new SampleResult();

        result.sampleStart();

        NettySerializableTransaction transaction = new NettySerializableTransaction();

        boolean commit;

        do {
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

        return result;
    }

}