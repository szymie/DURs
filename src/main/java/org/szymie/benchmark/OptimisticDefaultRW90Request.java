package org.szymie.benchmark;

import org.apache.jmeter.config.Arguments;

public class OptimisticDefaultRW90Request extends BaseOptimisticJMeterRequest implements DefaultRW90Request {

    @Override
    public Arguments getDefaultParameters() {
        return DefaultRW90Request.super.getDefaultParameters();
    }
}
