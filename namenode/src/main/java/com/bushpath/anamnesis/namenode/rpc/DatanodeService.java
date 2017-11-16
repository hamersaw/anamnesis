package com.bushpath.anamnesis.namenode.rpc;

import com.google.protobuf.Message;
import org.apache.hadoop.hdfs.protocol.proto.DatanodeProtocolProtos;
import org.apache.hadoop.hdfs.protocol.proto.HdfsProtos;
import org.apache.hadoop.hdfs.protocol.proto.HdfsServerProtos;

import com.bushpath.anamnesis.namenode.DatanodeManager;

import java.io.DataInputStream;
import java.util.ArrayList;
import java.util.List;

public class DatanodeService {
    private DatanodeManager datanodeManager;    

    public DatanodeService(DatanodeManager datanodeManager) {
        this.datanodeManager = datanodeManager;
    }

    public Message registerDatanode(DataInputStream in) throws Exception {
        DatanodeProtocolProtos.RegisterDatanodeRequestProto req =
            DatanodeProtocolProtos.RegisterDatanodeRequestProto.parseDelimitedFrom(in);
        
        // register datanode
        DatanodeProtocolProtos.DatanodeRegistrationProto registration 
            = req.getRegistration();
        HdfsProtos.DatanodeIDProto datanodeID = registration.getDatanodeID();

        this.datanodeManager.registerDatanode(datanodeID.getIpAddr(),
            datanodeID.getHostName(), datanodeID.getDatanodeUuid(),
            datanodeID.getXferPort(), datanodeID.getInfoPort(),
            datanodeID.getIpcPort(), System.currentTimeMillis());

        // send response
        return DatanodeProtocolProtos.RegisterDatanodeResponseProto.newBuilder()
            .setRegistration(req.getRegistration()) // TODO - change?
            .build();
    }

    public Message sendHeartbeat(DataInputStream in) throws Exception {
        DatanodeProtocolProtos.HeartbeatRequestProto req =
            DatanodeProtocolProtos.HeartbeatRequestProto.parseDelimitedFrom(in);

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

        return DatanodeProtocolProtos.HeartbeatResponseProto.newBuilder()
            .addAllCmds(cmds)
            .setHaStatus(status)
            .build();
    }

    public Message blockReport(DataInputStream in) throws Exception {
        DatanodeProtocolProtos.BlockReportRequestProto req =
            DatanodeProtocolProtos.BlockReportRequestProto.parseDelimitedFrom(in);

        // TODO handle block reports

        return DatanodeProtocolProtos.BlockReportResonseProto.newBuilder()
            .build();
    }
}
