package com.bushpath.anamnesis.client;

import com.bushpath.anamnesis.client.protocol.ClientNamenodeClient;

public class DFSClient {
    private ClientNamenodeClient clientNamenodeClient;

    public DFSClient(String ipAddr, int port) {
        this.clientNamenodeClient = new ClientNamenodeClient(ipAddr, port);
    }

    public void mkdir(String path) {
        //rpc mkdirs(MkdirsRequestProto) returns(MkdirsResponseProto);
    }

    public void create(String path) {

    }
}
