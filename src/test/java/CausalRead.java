import org.szymie.Configuration;
import org.szymie.client.strong.causal.NettyCausalTransaction;
import org.szymie.client.strong.causal.Session;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class CausalRead {

    public static void main(String[] args) {

        Map<String, String> properties = new HashMap<>();

        properties.put("replicas", "0-127.0.0.1:8080");

        Configuration configuration = new Configuration(properties);

        Session session = new Session();

        NettyCausalTransaction transaction = session.newTransaction(configuration);

        transaction.begin();

        int empties = 0;

        for(int i = 0; i < 2000; i++) {
            List<String> read = transaction.read("key" + i);

            if(read.isEmpty()) {
                empties++;
            }

        }

        transaction.commit();

        System.err.println("Empties: " + empties);
    }
}
