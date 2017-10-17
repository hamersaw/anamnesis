package com.bushpath.anamnesis.namenode.protocol;

import io.grpc.stub.StreamObserver;
import org.apache.hadoop.hdfs.protocol.proto.DatanodeProtocolProtos;
import org.apache.hadoop.hdfs.protocol.proto.DatanodeProtocolServiceGrpc;
import org.apache.hadoop.hdfs.protocol.proto.HdfsServerProtos;

import com.bushpath.anamnesis.namenode.DatanodeManager;
import com.bushpath.anamnesis.protocol.DatanodeProtocol;
import com.bushpath.anamnesis.protocol.HdfsServer;

import java.util.ArrayList;
import java.util.List;
import java.util.logging.Logger;

public class DatanodeService
        extends DatanodeProtocolServiceGrpc.DatanodeProtocolServiceImplBase {
    private static final Logger logger =
        Logger.getLogger(DatanodeService.class.getName());

    private DatanodeManager datanodeManager;

    public DatanodeService(DatanodeManager datanodeManager) {
        this.datanodeManager = datanodeManager;
    }

    @Override
    public void registerDatanode(DatanodeProtocolProtos.RegisterDatanodeRequestProto req,
            StreamObserver<DatanodeProtocolProtos.RegisterDatanodeResponseProto> 
            responseObserver) {
        logger.info("TODO - registering datanode");
        this.datanodeManager.processRegistration(req);

        // TODO - do some stuff eh
        DatanodeProtocolProtos.RegisterDatanodeResponseProto response =
            DatanodeProtocol.buildRegisterDatanodeResponseProto(req.getRegistration());

        responseObserver.onNext(response);
        responseObserver.onCompleted();
    }

    @Override
    public void sendHeartbeat(DatanodeProtocolProtos.HeartbeatRequestProto req,
            StreamObserver<DatanodeProtocolProtos.HeartbeatResponseProto>
            responseObserver) {
        logger.info("TODO - datanode heartbeat");
        this.datanodeManager.processHeartbeat(req);

        // TODO - do some stuff eh
        // can be empty
        List<DatanodeProtocolProtos.DatanodeCommandProto> cmds = new ArrayList<>();

        HdfsServerProtos.NNHAStatusHeartbeatProto status = 
            HdfsServer.buildNNHAStatusHeartbeatProto(
                HdfsServerProtos.NNHAStatusHeartbeatProto.State.ACTIVE, 0);

        DatanodeProtocolProtos.HeartbeatResponseProto response =
            DatanodeProtocol.buildHeartbeatResponseProto(cmds, status);

        responseObserver.onNext(response);
        responseObserver.onCompleted();
    }
}
