package com.bushpath.anamnesis.namenode;

import java.io.FileInputStream;
import java.util.Properties;

public class Configuration {
    public int port;
    public long blockSize;
    public int writePacketSize, replication, fileBufferSize;

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
        blockSize = Long.parseLong(properties.getProperty("blockSize"));
        writePacketSize = Integer.parseInt(properties.getProperty("writePacketSize"));
        replication = Integer.parseInt(properties.getProperty("replication"));
        fileBufferSize = Integer.parseInt(properties.getProperty("fileBufferSize"));
    }
}
