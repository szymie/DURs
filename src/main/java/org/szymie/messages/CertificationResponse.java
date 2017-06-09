package org.szymie.messages;

import java.io.Serializable;

public class CertificationResponse implements Serializable {

    public final boolean success;

    public CertificationResponse(boolean success) {
        this.success = success;
    }
}
