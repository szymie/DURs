package org.szymie.benchmark;

import org.apache.jmeter.config.Arguments;
import org.apache.jmeter.protocol.java.sampler.JavaSamplerContext;
import org.apache.jmeter.samplers.SampleResult;
import org.szymie.client.strong.optimistic.NettySerializableTransaction;

public class OptimisticRWJMeterRequest extends BaseRWJMeterRequest {

    @Override
    public SampleResult runTest(JavaSamplerContext javaSamplerContext) {

        resetStatisticsValues();

        //System.err.println("start");

        SampleResult result = new SampleResult();

        result.sampleStart();

        NettySerializableTransaction transaction = new NettySerializableTransaction(numberOfClientThreads, configuration);

        boolean commit;

        do {

            transaction.begin();

            //System.err.println("executeOperations");

            executeOperations(transaction);

            commit = transaction.commit();

            attempts++;

        } while (!commit);

        //System.err.println("committed");

        result.sampleEnd();
        result.setSuccessful(true);

        if(attempts > 1) {
            aborted = true;
            /*result.setErrorCount(1);
            result.setSuccessful(false);*/
        }

        result.setResponseMessage(createResponseMessage());

        return result;
    }
}
