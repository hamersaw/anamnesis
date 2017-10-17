package com.bushpath.anamnesis.namenode.protocol;

import io.grpc.stub.StreamObserver;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolGrpc;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos;

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

    @Override
    public void create(ClientNamenodeProtocolProtos.CreateRequestProto req,
            StreamObserver<ClientNamenodeProtocolProtos.CreateResponseProto>
            responseObserver) {

        responseObserver.onCompleted();
    }

    @Override
    public void mkdirs(ClientNamenodeProtocolProtos.MkdirsRequestProto req,
            StreamObserver<ClientNamenodeProtocolProtos.MkdirsResponseProto>
            responseObserver) {

        responseObserver.onCompleted();
    }
}
