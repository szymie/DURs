package org.szymie.benchmark;

import org.apache.jmeter.config.Arguments;

public class PessimisticDefaultRW20Request extends BasePessimisticJMeterRequest implements DefaultRW20Request {

    @Override
    public Arguments getDefaultParameters() {
        return DefaultRW20Request.super.getDefaultParameters();
    }
}
