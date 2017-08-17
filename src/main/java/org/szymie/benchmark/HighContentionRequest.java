package org.szymie.benchmark;

import org.apache.jmeter.config.Arguments;

public interface HighContentionRequest {

    default Arguments getDefaultParameters() {

        Arguments params = new Arguments();

        params.addArgument("numberOfReadsInQuery", "100");
        params.addArgument("numberOfReads", "40");
        params.addArgument("numberOfWrites", "10");
        params.addArgument("delayInMillis", "0");
        params.addArgument("numberOfKeys", "300");

        

        return params;
    }

}
