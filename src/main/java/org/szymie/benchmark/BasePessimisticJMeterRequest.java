package org.szymie.benchmark;

import com.fasterxml.jackson.databind.ser.Serializers;
import org.apache.jmeter.config.Arguments;
import org.apache.jmeter.protocol.java.sampler.JavaSamplerContext;
import org.apache.jmeter.samplers.SampleResult;
import org.szymie.Benchmark;
import org.szymie.client.strong.pessimistic.NettySerializableTransaction;

public abstract class BasePessimisticJMeterRequest extends BaseJMeterRequest {

    @Override
    public SampleResult runTest(JavaSamplerContext javaSamplerContext) {

        SampleResult result = new SampleResult();

        result.sampleStart();

        NettySerializableTransaction transaction = new NettySerializableTransaction();

        transaction.begin(reads, writes);

        long timestamp = transaction.getTimestamp();

        System.err.println(timestamp + " started");

        executeOperations(transaction);

        /*if(delayInMillis != 0) {
            try {
                Thread.sleep(delayInMillis);
            } catch (InterruptedException ignore) { }
        }*/

        System.err.println(timestamp + " trying to commit");

        transaction.commit();

        System.err.println(timestamp + " committed");

        result.sampleEnd();
        result.setSuccessful(true);

        return result;
    }
}
