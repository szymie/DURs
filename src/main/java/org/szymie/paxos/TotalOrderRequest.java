package org.szymie.paxos;

import java.io.*;

public class TotalOrderRequest implements Serializable {

    public long id;

    public TotalOrderRequest(long id) {
        this.id = id;
    }

    public byte[] toByteArray() {

        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        ObjectOutput out = null;

        try {
            out = new ObjectOutputStream(bos);
            out.writeObject(this);
            out.flush();
            return bos.toByteArray();
        } catch (IOException e) {
            throw new RuntimeException(e.getMessage());
        } finally {

            if(out != null) {
                try {
                    out.close();
                } catch (IOException ignored) {
                }
            }

            try {
                bos.close();
            } catch (IOException ignored) {
            }
        }
    }
}
