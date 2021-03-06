package org.szymie;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.*;

public class Configuration {

    private String fileName;
    private Properties properties;
    private Random random;

    public Configuration() {
        fileName = "config.properties";
        properties = new Properties();
        random = new Random(System.currentTimeMillis());
    }

    public Configuration(String fileName) {
        this();
        this.fileName = fileName;
    }

    public List<String> getAsList(String key) {
        return new ArrayList<>(Arrays.asList(get(key).split(",")));
    }

    public String get(String key) {

        InputStream inputStream = getClass().getClassLoader().getResourceAsStream(fileName);

        try {

            if(inputStream == null) {
                throw new RuntimeException(new FileNotFoundException("config file '" + fileName + "' not found in the classpath"));
            }

            properties.load(inputStream);
            return properties.getProperty(key);
        } catch (IOException e) {
            throw new RuntimeException(e);
        } finally {

            if(inputStream != null) {
                try {
                    inputStream.close();
                } catch (IOException ignore) {
                }
            }
        }
    }

    public Map.Entry<Integer, String> getRandomReplicaEndpoint() {
        List<String> replicas = getAsList("replicas");
        return getRandomElement(replicas);
    }

    public Map.Entry<Integer, String> getRandomElement(List<String> list) {
        int index = random.nextInt(list.size());
        String[] replica = list.get(index).split("-");
        return new AbstractMap.SimpleEntry<>(Integer.parseInt(replica[0]), replica[1]);
    }
}