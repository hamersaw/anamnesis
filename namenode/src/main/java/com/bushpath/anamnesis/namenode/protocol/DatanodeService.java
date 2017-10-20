package com.bushpath.anamnesis.namenode.protocol;

import io.grpc.stub.StreamObserver;
import org.apache.hadoop.hdfs.protocol.proto.DatanodeProtocolProtos;
import org.apache.hadoop.hdfs.protocol.proto.DatanodeProtocolServiceGrpc;
import org.apache.hadoop.hdfs.protocol.proto.HdfsProtos;
import org.apache.hadoop.hdfs.protocol.proto.HdfsServerProtos;

import com.bushpath.anamnesis.namenode.DatanodeManager;

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
        logger.info("recv register datanode");
        
        try {
            // register datanode
            DatanodeProtocolProtos.DatanodeRegistrationProto registration 
                = req.getRegistration();
            HdfsProtos.DatanodeIDProto datanodeID = registration.getDatanodeID();

            this.datanodeManager.registerDatanode(datanodeID.getIpAddr(),
                datanodeID.getHostName(), datanodeID.getDatanodeUuid(),
                datanodeID.getXferPort(), datanodeID.getInfoPort(),
                datanodeID.getIpcPort(), System.currentTimeMillis());

            // send response
            DatanodeProtocolProtos.RegisterDatanodeResponseProto response =
                DatanodeProtocolProtos.RegisterDatanodeResponseProto.newBuilder()
                    .setRegistration(req.getRegistration()) // TODO - change?
                    .build();

            responseObserver.onNext(response);
            responseObserver.onCompleted();
        } catch (Exception e) {
            logger.severe(e.toString());
            responseObserver.onError(e);
        }
    }

    @Override
    public void sendHeartbeat(DatanodeProtocolProtos.HeartbeatRequestProto req,
            StreamObserver<DatanodeProtocolProtos.HeartbeatResponseProto>
            responseObserver) {
        logger.info("recv send heartbeat");

        try {
            // update datanode
            DatanodeProtocolProtos.DatanodeRegistrationProto registration
                = req.getRegistration();
            HdfsProtos.DatanodeIDProto datanodeID = registration.getDatanodeID();

            this.datanodeManager.updateDatanode(datanodeID.getDatanodeUuid(),
                System.currentTimeMillis());

            // send response
            List<DatanodeProtocolProtos.DatanodeCommandProto> cmds = new ArrayList<>();

            HdfsServerProtos.NNHAStatusHeartbeatProto status = 
                HdfsServerProtos.NNHAStatusHeartbeatProto.newBuilder()
                    .setState(HdfsServerProtos.NNHAStatusHeartbeatProto.State.ACTIVE)
                    .setTxid(0)
                    .build();

            DatanodeProtocolProtos.HeartbeatResponseProto response =
                DatanodeProtocolProtos.HeartbeatResponseProto.newBuilder()
                    .addAllCmds(cmds)
                    .setHaStatus(status)
                    .build();

            responseObserver.onNext(response);
            responseObserver.onCompleted();
        } catch (Exception e) {
            logger.severe(e.toString());
            responseObserver.onError(e);
        }
    }
}
