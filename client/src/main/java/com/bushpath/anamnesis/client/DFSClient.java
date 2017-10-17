package com.bushpath.anamnesis.client;

import com.bushpath.anamnesis.client.protocol.ClientNamenodeClient;

import java.util.logging.Logger;

public class DFSClient {
    private static final Logger logger = Logger.getLogger(DFSClient.class.getName());
    private ClientNamenodeClient clientNamenodeClient;

    public DFSClient(String ipAddr, int port) {
        this.clientNamenodeClient = new ClientNamenodeClient(ipAddr, port);
    }

    public void mkdir(String path) {
        logger.info("creating directory '" + path + "'");
        this.clientNamenodeClient.mkdir(path, 65535, true);
    }

    public void create(String path) {

    }
}
