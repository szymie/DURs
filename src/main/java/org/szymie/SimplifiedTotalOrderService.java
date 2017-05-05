package org.szymie;

import lsr.service.SimplifiedService;
import org.apache.commons.lang.SerializationUtils;

import java.io.*;
import java.util.HashMap;
import java.util.Map;

public class SimplifiedTotalOrderService extends SimplifiedService {

    private long number = 0;

    @Override
    protected byte[] execute(byte[] bytes) {

        TotalOrderRequest totalOrderRequest = (TotalOrderRequest) SerializationUtils.deserialize(bytes);

        long clientId = totalOrderRequest.id;

        System.out.println("Client's id: " + clientId + ", number: " + ++number);

        return SerializationUtils.serialize(number);
    }

    @Override
    protected byte[] makeSnapshot() {

        ByteArrayOutputStream stream = new ByteArrayOutputStream();

        try {
            ObjectOutputStream objectOutputStream = new ObjectOutputStream(stream);
            objectOutputStream.writeLong(number);
        } catch (IOException e) {
            throw new RuntimeException("Snapshot creation error");
        }

        System.out.println("makeSnapshot");

        return stream.toByteArray();
    }

    @Override
    protected void updateToSnapshot(byte[] snapshot) {

        ByteArrayInputStream stream = new ByteArrayInputStream(snapshot);
        ObjectInputStream objectInputStream;

        try {
            objectInputStream = new ObjectInputStream(stream);
            number = (Long) objectInputStream.readObject();
        } catch (Exception e) {
            throw new RuntimeException("Snapshot read error");
        }

        System.out.println("updateToSnapshot");
    }
}
