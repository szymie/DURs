package org.szymie.benchmark;

import org.apache.jmeter.config.Arguments;

public class PessimisticDefaultRW90Request extends BasePessimisticJMeterRequest implements DefaultRW90Request {

    @Override
    public Arguments getDefaultParameters() {
        return DefaultRW90Request.super.getDefaultParameters();
    }
}
