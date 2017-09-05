package org.szymie.messages;

import java.io.Serializable;

public class CausalCertificationResponse implements Serializable {

    public final boolean success;
    public final long sequentialNumber;

    public CausalCertificationResponse(boolean success, long sequentialNumber) {
        this.success = success;
        this.sequentialNumber = sequentialNumber;
    }
}
