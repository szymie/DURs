package org.szymie.messages;

import java.io.Serializable;

public class CommitResponse implements Serializable {

    private boolean success;

    public CommitResponse() {
    }

    public CommitResponse(boolean success) {
        this.success = success;
    }

    public boolean isSuccess() {
        return success;
    }

    public void setSuccess(boolean success) {
        this.success = success;
    }
}
