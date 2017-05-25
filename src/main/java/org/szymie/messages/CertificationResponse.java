package org.szymie.messages;

import java.io.Serializable;

public class CertificationResponse implements Serializable {

    public boolean success;

    public CertificationResponse(boolean success) {
        this.success = success;
    }
}
