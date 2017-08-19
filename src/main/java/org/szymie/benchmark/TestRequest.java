package org.szymie.benchmark;

import org.apache.jmeter.config.Arguments;
import org.apache.jmeter.protocol.java.sampler.JavaSamplerContext;
import org.apache.jmeter.samplers.SampleResult;

public class TestRequest extends BaseOptimisticJMeterRequest implements DefaultRW20Request {

    @Override
    public Arguments getDefaultParameters() {
        return DefaultRW20Request.super.getDefaultParameters();
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
