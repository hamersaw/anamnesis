package com.bushpath.anamnesis.client;

import com.bushpath.anamnesis.client.protocol.ClientNamenodeClient;

import java.util.logging.Logger;

public class DFSClient {
    private static final Logger logger = Logger.getLogger(DFSClient.class.getName());
    private static final long BLOCK_SIZE = 64000;
    private ClientNamenodeClient clientNamenodeClient;

    public DFSClient(String ipAddr, int port) {
        this.clientNamenodeClient = new ClientNamenodeClient(ipAddr, port);
    }

    public void create(String path) {
        logger.info("creating file '" + path + "'");
        this.clientNamenodeClient.create(path, Integer.MAX_VALUE, "DFSClient",
                Integer.MAX_VALUE, true, -1, this.BLOCK_SIZE);
    }

    public void ls(String path) {
        logger.info("get listings for '" + path + "'");
        this.clientNamenodeClient.getListing(path, new byte[]{}, false);
    }

    public void mkdir(String path) {
        logger.info("creating directory '" + path + "'");
        this.clientNamenodeClient.mkdir(path, Integer.MAX_VALUE, true);
    }
}
