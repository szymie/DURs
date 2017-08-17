package org.szymie.benchmark;

import org.apache.jmeter.config.Arguments;

public class OptimisticDefaultRW20Request extends BaseOptimisticJMeterRequest implements DefaultRW20Request {

    @Override
    public Arguments getDefaultParameters() {
        return DefaultRW20Request.super.getDefaultParameters();
    }
}
