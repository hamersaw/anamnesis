package com.bushpath.anamnesis.namenode.ipc.rpc;

import com.google.protobuf.Message;
import org.apache.hadoop.hdfs.protocol.proto.DatanodeProtocolProtos;
import org.apache.hadoop.hdfs.protocol.proto.HdfsProtos;
import org.apache.hadoop.hdfs.protocol.proto.HdfsServerProtos;

import com.bushpath.anamnesis.ipc.rpc.SocketContext;
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

    public Message registerDatanode(DataInputStream in,
            SocketContext socketContext) throws Exception {
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

    public Message sendHeartbeat(DataInputStream in,
            SocketContext socketContext) throws Exception {
        DatanodeProtocolProtos.HeartbeatRequestProto req =
            DatanodeProtocolProtos.HeartbeatRequestProto.parseDelimitedFrom(in);

        // update datanode
        DatanodeProtocolProtos.DatanodeRegistrationProto registration
            = req.getRegistration();
        HdfsProtos.DatanodeIDProto datanodeId = registration.getDatanodeID();

        this.datanodeManager.updateDatanode(datanodeId.getDatanodeUuid(),
            System.currentTimeMillis());

        // update datanode storages
        for (HdfsProtos.StorageReportProto storageReport : req.getReportsList()) {
            this.datanodeManager.updateDatanodeStorage(
                datanodeId.getDatanodeUuid(),
                storageReport.getStorage().getStorageUuid(),
                storageReport.getCapacity(),
                storageReport.getRemaining());
        }

        // check for differences in reported and actual managed block ids
        List<Long> reportedBlockIds =
            this.datanodeManager.get(datanodeId.getDatanodeUuid()).getReportedBlockIds();
        List<Block> blocks =
            this.blockManager.getDatanodeBlocks(datanodeId.getDatanodeUuid());
        List<HdfsProtos.BlockProto> removeBlocks = new ArrayList<>();
        for (Long reportedBlockId : reportedBlockIds) {
            boolean blockFound = false;
            for (Block block : blocks) {
                if (block.getBlockId() == reportedBlockId) {
                    blockFound = true;
                    break;
                }
            }

            if (!blockFound) {
                removeBlocks.add(
                    HdfsProtos.BlockProto.newBuilder()
                        .setBlockId(reportedBlockId)
                        .setGenStamp(-1)
                        .build()
                    );
            }
        }

        // send response
        List<DatanodeProtocolProtos.DatanodeCommandProto> cmds = new ArrayList<>();

        // if there are block ids to remove add block command
        if (!removeBlocks.isEmpty()) {
            DatanodeProtocolProtos.DatanodeCommandProto.Type cmdType =
                DatanodeProtocolProtos.DatanodeCommandProto.Type.BlockCommand;

            DatanodeProtocolProtos.BlockCommandProto blkCmd =
                DatanodeProtocolProtos.BlockCommandProto.newBuilder()
                    .setAction(DatanodeProtocolProtos.BlockCommandProto.Action.INVALIDATE)
                    .setBlockPoolId("") // TODO - set block pool id
                    .addAllBlocks(removeBlocks)
                    .build();

            DatanodeProtocolProtos.DatanodeCommandProto datanodeCommand =
                DatanodeProtocolProtos.DatanodeCommandProto.newBuilder()
                    .setCmdType(cmdType)
                    .setBlkCmd(blkCmd)
                    .build();

            cmds.add(datanodeCommand);
        }

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

    public Message blockReport(DataInputStream in,
            SocketContext socketContext) throws Exception {
        DatanodeProtocolProtos.BlockReportRequestProto req =
            DatanodeProtocolProtos.BlockReportRequestProto.parseDelimitedFrom(in);

        // retreive DatanodeId
        HdfsProtos.DatanodeIDProto datanodeIdProto =
            req.getRegistration().getDatanodeID();
        Datanode datanode = 
            this.datanodeManager.get(datanodeIdProto.getDatanodeUuid());
        
        // iterate over storage block reports
        List<DatanodeProtocolProtos.StorageBlockReportProto> storageReports = 
            req.getReportsList();
        List<Long> reportedBlockIds = new ArrayList<>();
        for (DatanodeProtocolProtos.StorageBlockReportProto storageReport:
                storageReports) {

            // iterate over blocks in report
            List<Long> blockList = storageReport.getBlocksList();
            for (int i=0; i<blockList.size(); i += 4) {
                // add block to reported block ids list
                reportedBlockIds.add(blockList.get(i));

                // check if block still exists
                if (!this.blockManager.contains(blockList.get(i))) {
                    continue;
                }

                // update block
                Block block = this.blockManager.get(blockList.get(i));
                block.setLength(blockList.get(i + 1));
                block.getFile().updateBlockOffsets();

                // add block location
                HdfsProtos.DatanodeStorageProto datanodeStorage =
                    storageReport.getStorage();
                //block.addLoc(datanode, true, datanodeStorage.getStorageType(),
                //    datanodeStorage.getStorageUuid());
                block.addLoc(datanode, false, datanodeStorage.getStorageType(),
                    datanodeStorage.getStorageUuid());
            }
        }

        // set datanode reported block ids
        datanode.setReportedBlockIds(reportedBlockIds);
        
        return DatanodeProtocolProtos.BlockReportResponseProto.newBuilder()
            .build();
    }
}
