package org.szymie.benchmark;

import org.apache.jmeter.protocol.java.sampler.JavaSamplerContext;
import org.apache.jmeter.samplers.SampleResult;
import org.szymie.client.strong.ReadWriteRemoveCommitTransaction;
import org.szymie.client.strong.causal.NettyCausalTransaction;
import org.szymie.client.strong.causal.Session;

import java.util.List;
import java.util.Map;

public class CausalROJMeterRequest extends BaseROJMeterRequest {

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

        for(Map.Entry<String, Integer> read : reads.entrySet()) {

            List<String> values = transaction.read(read.getKey());

            long valuesSize = values.size();

            numberOfReads++;

            if(valuesSize > 1) {
                numberOfMultiValueReads++;
                totalMultiValueReadSize += valuesSize;
            }

            //System.err.println(read.getKey() + " " + String.join(", ", value));
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
