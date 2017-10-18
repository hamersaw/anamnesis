package com.bushpath.anamnesis.datanode;

import java.io.FileInputStream;
import java.util.Properties;

public class Configuration {
    public String hostname, ipAddr, datanodeUuid, clusterId, namenodeIpAddr;
    public int xferPort, ipcPort, infoPort, namespceId, namenodePort;

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
        hostname = properties.getProperty("hostname");
        ipAddr = properties.getProperty("ipAddr");
        xferPort = Integer.parseInt(properties.getProperty("xferPort"));
        ipcPort = Integer.parseInt(properties.getProperty("ipcPort"));
        infoPort = Integer.parseInt(properties.getProperty("infoPort"));

        datanodeUuid = properties.getProperty("datanodeUuid");
        clusterId = properties.getProperty("clusterId");
        namespceId = Integer.parseInt(properties.getProperty("namespceId"));

        namenodeIpAddr = properties.getProperty("namenodeIpAddr");
        namenodePort = Integer.parseInt(properties.getProperty("namenodePort"));
    }
}
