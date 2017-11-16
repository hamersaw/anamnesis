package com.bushpath.anamnesis.datanode;

import java.io.FileInputStream;
import java.util.Properties;

public class Configuration {
    public String hostname;
    public String ipAddr;
    public int xferPort;
    public int ipcPort;
    public int infoPort;

    public String datanodeUuid;
    public String clusterId;
    public int namespceId;
    public String poolId;
    public String storage;
    public String storageUuid;

    public String namenodeIpAddr;
    public int namenodePort;

    public int blockReportInterval;
    public int heartbeatInterval;

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
        poolId = properties.getProperty("poolId");
        storage = properties.getProperty("storage");
        storageUuid = properties.getProperty("storageUuid");

        namenodeIpAddr = properties.getProperty("namenodeIpAddr");
        namenodePort = Integer.parseInt(properties.getProperty("namenodePort"));

        blockReportInterval =
            Integer.parseInt(properties.getProperty("blockReportInterval"));
        heartbeatInterval =
            Integer.parseInt(properties.getProperty("heartbeatInterval"));
    }
}
