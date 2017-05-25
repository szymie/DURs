package org.szymie;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

public class Configuration {

    private String fileName;
    private Properties properties;

    public Configuration() {
        fileName = "config.properties";
        properties = new Properties();
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
}