package org.szymie;


import lsr.common.Configuration;
import lsr.paxos.client.Client;
import lsr.paxos.client.ReplicationException;
import org.apache.commons.lang.SerializationUtils;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.util.Scanner;

public class ClientMain {

    public static void main(String[] args) throws IOException, ReplicationException {

        Scanner scanner = new Scanner(System.in);
        long id = scanner.nextLong();

        Configuration conf = new Configuration("src/main/resources/paxos.properties");

        Client client = new Client(conf);
        client.connect();

        TotalOrderRequest request = new TotalOrderRequest(id);
        byte[] requestBytes = request.toByteArray();

        byte[] response = client.execute(requestBytes);

        Long responseNumber = (Long) SerializationUtils.deserialize(response);

        //DataInputStream in = new DataInputStream(new ByteArrayInputStream(response));
        //System.out.println(id + ": " + in.readLong());
        System.out.println(id + ": " + responseNumber);
    }
}
