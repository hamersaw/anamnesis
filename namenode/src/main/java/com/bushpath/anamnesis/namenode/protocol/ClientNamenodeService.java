package com.bushpath.anamnesis.namenode.protocol;

import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolGrpc;

import com.bushpath.anamnesis.namenode.DatanodeManager;

import java.util.logging.Logger;

public class ClientNamenodeService
    extends ClientNamenodeProtocolGrpc.ClientNamenodeProtocolImplBase {
    private static final Logger logger =
        Logger.getLogger(ClientNamenodeService.class.getName());

    private DatanodeManager datanodeManager;

    public ClientNamenodeService(DatanodeManager datanodeManager) {
        this.datanodeManager = datanodeManager;
    }
}
