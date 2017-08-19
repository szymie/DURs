package org.szymie.benchmark;

import org.apache.jmeter.config.Arguments;
import org.apache.jmeter.protocol.java.sampler.AbstractJavaSamplerClient;
import org.apache.jmeter.protocol.java.sampler.JavaSamplerContext;
import org.apache.jmeter.samplers.SampleResult;
import org.apache.jmeter.testelement.property.JMeterProperty;
import org.apache.jmeter.threads.JMeterContextService;

public class TestRequest2 extends AbstractJavaSamplerClient {

    @Override
    public Arguments getDefaultParameters() {

        Arguments params = new Arguments();

        params.addArgument("replicas", "${replicas}");

        params.addArgument("paxosProcesses", "${paxosProcesses}");
        params.addArgument("numberOfReadsInQuery", "100");
        params.addArgument("numberOfReads", "8");
        params.addArgument("numberOfWrites", "2");
        params.addArgument("delayInMillis", "0");
        params.addArgument("numberOfKeys", "300");

        return params;
    }

    @Override
    public void setupTest(JavaSamplerContext context) {

        super.setupTest(context);

        String replicas = context.getParameter("replicas");
        String paxosProcesses = context.getParameter("paxosProcesses");

        if(replicas != null) {
            System.err.println("replicas: " + replicas);
        } else {
            System.err.println("null");

            JMeterProperty replicas1 = JMeterContextService.getContext().getCurrentSampler().getProperty("replicas");
            JMeterProperty replicas2 = JMeterContextService.getContext().getCurrentSampler().getProperty("${replicas}");

            System.err.println("replicas1: " + replicas1);
            System.err.println("replicas2: " + replicas2);

        }

        if(paxosProcesses != null) {
            System.err.println("paxosProcesses: " + paxosProcesses);
        } else {
            System.err.println("null");

            JMeterProperty paxosProcesses1 = JMeterContextService.getContext().getCurrentSampler().getProperty("paxosProcesses");
            JMeterProperty paxosProcesses2 = JMeterContextService.getContext().getCurrentSampler().getProperty("${paxosProcesses}");

            System.err.println("paxosProcesses: " + paxosProcesses1);
            System.err.println("paxosProcesses: " + paxosProcesses2);
        }
    }

    @Override
    public SampleResult runTest(JavaSamplerContext javaSamplerContext) {

        SampleResult result = new SampleResult();

        result.sampleStart();

        result.sampleEnd();
        result.setSuccessful(true);

        return result;
    }

}
