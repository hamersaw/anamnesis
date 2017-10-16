package com.bushpath.anamnesis.namenode.protocol;

import io.grpc.stub.StreamObserver;
import org.apache.hadoop.hdfs.protocol.proto.DatanodeProtocolProtos;
import org.apache.hadoop.hdfs.protocol.proto.DatanodeProtocolServiceGrpc;

import com.bushpath.anamnesis.protocol.DatanodeProtocol;

import java.util.logging.Logger;

public class DatanodeService
        extends DatanodeProtocolServiceGrpc.DatanodeProtocolServiceImplBase {
    private static final Logger logger =
        Logger.getLogger(DatanodeService.class.getName());

    @Override
    public void registerDatanode(DatanodeProtocolProtos.RegisterDatanodeRequestProto req,
            StreamObserver<DatanodeProtocolProtos.RegisterDatanodeResponseProto> 
            responseObserver) {
        logger.info("TODO - registering datanode");

        // TODO - do some stuff eh
        DatanodeProtocolProtos.RegisterDatanodeResponseProto response =
            DatanodeProtocol.buildRegisterDatanodeResponseProto(req.getRegistration());

        responseObserver.onNext(response);
        responseObserver.onCompleted();
    }
}
