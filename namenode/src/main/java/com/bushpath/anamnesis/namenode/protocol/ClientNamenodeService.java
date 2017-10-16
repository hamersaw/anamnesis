package com.bushpath.anamnesis.namenode.protocol;

import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolGrpc;

import com.bushpath.anamnesis.namenode.DatanodePool;

import java.util.logging.Logger;

public class ClientNamenodeService
    extends ClientNamenodeProtocolGrpc.ClientNamenodeProtocolImplBase {
    private static final Logger logger =
        Logger.getLogger(ClientNamenodeService.class.getName());

    private DatanodePool datanodePool;

    public ClientNamenodeService(DatanodePool datanodePool) {
        this.datanodePool = datanodePool;
    }
}
