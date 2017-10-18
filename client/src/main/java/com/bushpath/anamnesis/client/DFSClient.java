package com.bushpath.anamnesis.client;

import com.bushpath.anamnesis.client.protocol.ClientNamenodeClient;

import java.util.logging.Logger;

public class DFSClient {
    private static final Logger logger = Logger.getLogger(DFSClient.class.getName());
    private ClientNamenodeClient clientNamenodeClient;

    public DFSClient(String ipAddr, int port) {
        this.clientNamenodeClient = new ClientNamenodeClient(ipAddr, port);
    }

    public void ls(String path) {
        logger.info("get listings for '" + path + "'");
        this.clientNamenodeClient.getListing(path, new byte[]{}, false);
    }

    public void mkdir(String path) {
        logger.info("creating directory '" + path + "'");
        this.clientNamenodeClient.mkdir(path, Integer.MAX_VALUE, true);
    }

    public void create(String path) {

    }
}
