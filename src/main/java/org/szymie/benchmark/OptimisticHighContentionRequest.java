package org.szymie.benchmark;

import org.apache.jmeter.config.Arguments;

public class OptimisticHighContentionRequest extends BaseOptimisticJMeterRequest implements HighContentionRequest {

    @Override
    public Arguments getDefaultParameters() {
        return HighContentionRequest.super.getDefaultParameters();
    }
}
