package com.bushpath.anamnesis.datanode;

import org.apache.hadoop.hdfs.protocol.proto.DatanodeProtocolProtos;
import org.apache.hadoop.hdfs.protocol.proto.HdfsProtos;

import com.bushpath.anamnesis.ipc.rpc.RpcClient;

import java.io.DataInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.TimerTask;

public class HeartbeatTask extends TimerTask {
    private Configuration config;

    public HeartbeatTask(Configuration config) {
        this.config = config;
    }

    @Override
    public void run() {
        RpcClient rpcClient = null;
        try {
            List<HdfsProtos.StorageReportProto> reports = new ArrayList<>();

            // TODO - construct storage reports
            
            DatanodeProtocolProtos.HeartbeatRequestProto req = 
                DatanodeProtocolProtos.HeartbeatRequestProto.newBuilder()
                    .setRegistration(Main.buildDatanodeRegistrationProto(this.config))
                    .addAllReports(reports)
                    .build();

            rpcClient = new RpcClient(config.namenodeIpAddr,
                config.namenodePort, "datanode",
                "org.apache.hadoop.hdfs.server.protocol.DatanodeProtocol");

            DataInputStream in = rpcClient.send("sendHeartbeat", req);

            // TODO - handle response
            DatanodeProtocolProtos.HeartbeatResponseProto response =
                DatanodeProtocolProtos.HeartbeatResponseProto.parseDelimitedFrom(in);
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
