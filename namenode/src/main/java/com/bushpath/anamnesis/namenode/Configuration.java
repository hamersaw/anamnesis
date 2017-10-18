package com.bushpath.anamnesis.namenode;

import java.io.FileInputStream;
import java.util.Properties;

public class Configuration {
    public int port;

    public Configuration(String filename) throws Exception {
        // read file into properties
        Properties properties = new Properties();
        FileInputStream input = new FileInputStream(filename); 
        try {
            properties.load(input);
        } finally {
            if (input != null) {
                input.close();
            }
        }

        // parse properties
        port = Integer.parseInt(properties.getProperty("port"));
    }
}
