package com.bushpath.anamnesis.namenode.ipc.rpc;

import com.google.protobuf.Message;
import org.apache.hadoop.hdfs.protocol.proto.DatanodeProtocolProtos;
import org.apache.hadoop.hdfs.protocol.proto.HdfsProtos;
import org.apache.hadoop.hdfs.protocol.proto.HdfsServerProtos;

import com.bushpath.anamnesis.namenode.Block;
import com.bushpath.anamnesis.namenode.BlockManager;
import com.bushpath.anamnesis.namenode.Datanode;
import com.bushpath.anamnesis.namenode.DatanodeManager;

import java.io.DataInputStream;
import java.util.ArrayList;
import java.util.List;

public class DatanodeService {
    private BlockManager blockManager;
    private DatanodeManager datanodeManager;    

    public DatanodeService(BlockManager blockManager, DatanodeManager datanodeManager) {
        this.blockManager = blockManager;
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

        // update datanode storages
        for (HdfsProtos.StorageReportProto storageReport : req.getReportsList()) {
            this.datanodeManager.updateDatanodeStorage(
                datanodeID.getDatanodeUuid(),
                storageReport.getStorage().getStorageUuid(),
                storageReport.getCapacity(),
                storageReport.getRemaining());
        }

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

        // retreive DatanodeId
        HdfsProtos.DatanodeIDProto datanodeIdProto =
            req.getRegistration().getDatanodeID();
        
        // iterate over storage block reports
        List<DatanodeProtocolProtos.StorageBlockReportProto> storageReports = 
            req.getReportsList();
        for (DatanodeProtocolProtos.StorageBlockReportProto storageReport:
                storageReports) {

            // iterate over blocks in report
            List<Long> blockList = storageReport.getBlocksList();
            for (int i=0; i<blockList.size(); i += 4) {
                // update block
                Block block = blockManager.get(blockList.get(i));
                block.setLength(blockList.get(i + 1));
                block.getFile().updateBlockOffsets();

                // add block location
                HdfsProtos.DatanodeStorageProto datanodeStorage =
                    storageReport.getStorage();
                Datanode datanode = 
                    this.datanodeManager.get(datanodeIdProto.getDatanodeUuid());
                block.addLoc(datanode, true, datanodeStorage.getStorageType(),
                    datanodeStorage.getStorageUuid());
            }
        }
        
        return DatanodeProtocolProtos.BlockReportResponseProto.newBuilder()
            .build();
    }
}
