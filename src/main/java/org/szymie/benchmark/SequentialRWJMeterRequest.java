package org.szymie.benchmark;



        import org.apache.jmeter.protocol.java.sampler.JavaSamplerContext;
        import org.apache.jmeter.samplers.SampleResult;
        import org.szymie.client.strong.pessimistic.NettySerializableTransaction;
        import org.szymie.client.strong.sequential.NettySequentialTransaction;

public class SequentialRWJMeterRequest extends BaseRWJMeterRequest {

    @Override
    public SampleResult runTest(JavaSamplerContext javaSamplerContext) {

        System.err.println("start");

        SampleResult result = new SampleResult();

        result.sampleStart();

        NettySequentialTransaction transaction = new NettySequentialTransaction(numberOfClientThreads, configuration);

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

        if(attempts > 1) {
            result.setErrorCount(1);
            result.setSuccessful(false);
        }

        return result;
    }
}
