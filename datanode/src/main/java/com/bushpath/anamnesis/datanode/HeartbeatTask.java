package com.bushpath.anamnesis.datanode;

import org.apache.hadoop.hdfs.protocol.proto.DatanodeProtocolProtos;
import org.apache.hadoop.hdfs.protocol.proto.HdfsProtos;

import com.bushpath.anamnesis.datanode.storage.Storage;
import com.bushpath.anamnesis.ipc.rpc.RpcClient;

import java.io.DataInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.TimerTask;

public class HeartbeatTask extends TimerTask {
    private Configuration config;
    private Storage storage;

    public HeartbeatTask(Configuration config, Storage storage) {
        this.config = config;
        this.storage = storage;
    }

    @Override
    public void run() {
        RpcClient rpcClient = null;
        try {
            // construct storage reports
            List<HdfsProtos.StorageReportProto> reports = new ArrayList<>();
            reports.add(this.storage.toStorageReportProto());

            // build request protobufs
            DatanodeProtocolProtos.HeartbeatRequestProto req = 
                DatanodeProtocolProtos.HeartbeatRequestProto.newBuilder()
                    .setRegistration(Main.buildDatanodeRegistrationProto(this.config))
                    .addAllReports(reports)
                    .build();

            // send request
            rpcClient = new RpcClient(config.namenodeIpAddr,
                config.namenodePort, "datanode",
                "org.apache.hadoop.hdfs.server.protocol.DatanodeProtocol");

            DataInputStream in = rpcClient.send("sendHeartbeat", req);

            // TODO - handle response
            DatanodeProtocolProtos.HeartbeatResponseProto response =
                DatanodeProtocolProtos.HeartbeatResponseProto.parseDelimitedFrom(in);

            // execute commands in response
            for (DatanodeProtocolProtos.DatanodeCommandProto datanodeCommand :
                    response.getCmdsList()) {
                switch (datanodeCommand.getCmdType()) {
                case BlockCommand :
                    // parse block command
                    DatanodeProtocolProtos.BlockCommandProto blockCommand =
                        datanodeCommand.getBlkCmd();

                    switch (blockCommand.getAction()) {
                    case INVALIDATE:
                        for (HdfsProtos.BlockProto block : blockCommand.getBlocksList()) {
                            this.storage.deleteBlock(block.getBlockId());
                        }

                        break;
                    default:
                        System.err.println("Unsupported block command action '" 
                            + blockCommand.getAction() + "'");
                    }

                    break;
                default:
                    System.err.println("Unsupported datanode command type '" 
                        + datanodeCommand.getCmdType() + "'");
                }
            }
        } catch(Exception e) {
            e.printStackTrace();
            System.err.println("failed to send datanode heartbeat: " + e);
        } finally {
            if (rpcClient != null) {
                try {
                    rpcClient.close();
                } catch(IOException e) {
                    System.err.println("failed to close rpc client: " + e);
                }
            }
        }
    }
}
