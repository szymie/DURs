package org.szymie.benchmark;

import org.apache.jmeter.config.Arguments;

public interface DefaultRW20Request {

    default Arguments getDefaultParameters() {

        Arguments params = new Arguments();

        params.addArgument("numberOfReadsInQuery", "100");
        params.addArgument("numberOfReads", "8");
        params.addArgument("numberOfWrites", "2");
        params.addArgument("delayInMillis", "0");
        params.addArgument("numberOfKeys", "300");

        return params;
    }
}
