package org.szymie;

import akka.actor.ActorSystem;
import org.szymie.client.SerializableTransaction;

public class ClientMain {

    public static void main(String[] args) {

        ActorSystem actorSystem = ActorSystem.create();

        SerializableTransaction t = new SerializableTransaction(actorSystem);

        t.begin();
        //t.write("a", "2");

        String a = t.read("a");

        System.out.println(a);

        System.out.println(t.commit());
    }
}
