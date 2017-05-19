package org.szymie.client;

public class ValuesGateway {

    private TransactionMetadata transactionMetadata;
    private boolean sessionOpen;

    public ValuesGateway() {
        transactionMetadata = new TransactionMetadata();
        sessionOpen = false;
    }

    public void openSession() {



        sessionOpen = true;
    }

    public void closeSession() {

        sessionOpen = false;
    }

    public boolean isSessionOpen() {
        return sessionOpen;
    }

    public String read(String key) {

        String value = transactionMetadata.writtenValues.get(key);

        if(value == null) {
            value = transactionMetadata.readValues.get(key);
        }

        if(value == null) {

            //get value with timestamp remotely
        }

        transactionMetadata.readValues.put(key, value);

        return value;
    }

    public void write(String key, String value) {
        transactionMetadata.writtenValues.put(key, value);
    }

    public void clear() {
        transactionMetadata.clear();
    }
}
