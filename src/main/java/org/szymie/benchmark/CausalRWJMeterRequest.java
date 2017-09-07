package org.szymie.benchmark;

import org.apache.jmeter.protocol.java.sampler.JavaSamplerContext;
import org.apache.jmeter.samplers.SampleResult;
import org.szymie.Benchmark;
import org.szymie.client.strong.causal.NettyCausalTransaction;
import org.szymie.client.strong.causal.Session;
import java.util.List;
import java.util.Map;

public class CausalRWJMeterRequest extends BaseRWJMeterRequest {

    @Override
    public SampleResult runTest(JavaSamplerContext javaSamplerContext) {

        resetStatisticsValues();

        SampleResult result = new SampleResult();

        result.sampleStart();

        Session session = new Session();

        NettyCausalTransaction transaction = session.newTransaction(numberOfClientThreads, configuration);

        transaction.begin();

        executeOperations(transaction);

        transaction.commit();

        attempts++;

        session.close();

        result.sampleEnd();

        result.setResponseMessage(createResponseMessage());
        result.setSuccessful(true);

        return result;
    }

    private void executeOperations(NettyCausalTransaction transaction) {

        for(Map.Entry<String, Integer> operationAndKey : operations.entrySet()) {

            String key = operationAndKey.getKey();
            int operation = operationAndKey.getValue();

            if((operation & Benchmark.Operations.READ) != 0) {

                List<String> values = transaction.read(key);

                long valuesSize = values.size();

                numberOfReads++;

                if(valuesSize > 1) {
                    totalMultiValueReadSize += valuesSize;
                    numberOfMultiValueReads++;
                }

                //System.err.println(key + " " + String.join(", ", values));
            }

            if((operation & Benchmark.Operations.WRITE) != 0) {
                transaction.write(key, String.valueOf(random.nextInt(numberOfKeys)));
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
