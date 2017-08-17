package org.szymie.benchmark;

import org.apache.jmeter.config.Arguments;

public interface DefaultRW90Request {

    default Arguments getDefaultParameters() {

        Arguments params = new Arguments();

        params.addArgument("numberOfReadsInQuery", "100");
        params.addArgument("numberOfReads", "1");
        params.addArgument("numberOfWrites", "9");
        params.addArgument("delayInMillis", "0");
        params.addArgument("numberOfKeys", "300");

        return params;
    }
}
