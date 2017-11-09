package com.bushpath.anamnesis.datanode;

import org.apache.hadoop.hdfs.protocol.proto.DatanodeProtocolProtos;
import org.apache.hadoop.hdfs.protocol.proto.HdfsProtos;
import org.apache.hadoop.hdfs.protocol.proto.HdfsServerProtos;

import com.bushpath.anamnesis.rpc.RpcClient;

import java.util.ArrayList;
import java.util.List;
import java.util.Timer;
import java.util.TimerTask;

public class HeartbeatManager {
    private RpcClient rpcClient;
    private Configuration config;
    private HdfsProtos.DatanodeIDProto datanodeIdProto;

    public HeartbeatManager(Configuration config) {
        this.config = config;
        this.datanodeIdProto = HdfsProtos.DatanodeIDProto.newBuilder()
            .setIpAddr(config.ipAddr)
            .setHostName(config.hostname)
            .setDatanodeUuid(config.datanodeUuid)
            .setXferPort(config.xferPort)
            .setInfoPort(config.infoPort)
            .setIpcPort(config.ipcPort)
            .build();

        // send initial register message
        DatanodeProtocolProtos.RegisterDatanodeRequestProto req =
            DatanodeProtocolProtos.RegisterDatanodeRequestProto.newBuilder()
                .setRegistration(buildDatanodeRegistrationProto())
                .build();

        // send rpc request
        try {
            this.rpcClient = new RpcClient(config.namenodeIpAddr,
                config.namenodePort, "datanode",
                "org.apache.hadoop.hdfs.server.protocol.DatanodeProtocol");
            byte[] respBuf = rpcClient.send("registerDatanode", req);

            // TODO - handle response
            DatanodeProtocolProtos.RegisterDatanodeResponseProto response =
                DatanodeProtocolProtos.RegisterDatanodeResponseProto.parseFrom(respBuf);
        } catch(Exception e) {
            System.err.println("failed to register datanode: " + e);
        }

        // start heartbeat timer
        Timer timer = new Timer(true);
        timer.scheduleAtFixedRate(new HeartbeatTask(config), 0, 5 * 1000);
    }

    private DatanodeProtocolProtos.DatanodeRegistrationProto 
        buildDatanodeRegistrationProto() {

        HdfsServerProtos.StorageInfoProto storageInfoProto = 
            HdfsServerProtos.StorageInfoProto.newBuilder()
                .setLayoutVersion(0)
                .setNamespceID(this.config.namespceId)
                .setClusterID(this.config.clusterId)
                .setCTime(System.currentTimeMillis())
                .build();

        HdfsServerProtos.BlockKeyProto currentKey =
            HdfsServerProtos.BlockKeyProto.newBuilder()
                .setKeyId(0)
                .setExpiryDate(-1)
                .build();

        List<HdfsServerProtos.BlockKeyProto> allKeys = new ArrayList<>();

        HdfsServerProtos.ExportedBlockKeysProto exportedBlockKeysProto =
            HdfsServerProtos.ExportedBlockKeysProto.newBuilder()
                .setIsBlockTokenEnabled(false)
                .setKeyUpdateInterval(5000)
                .setTokenLifeTime(5000)
                .setCurrentKey(currentKey)
                .addAllAllKeys(allKeys)
                .build();

        return DatanodeProtocolProtos.DatanodeRegistrationProto.newBuilder()
                .setDatanodeID(this.datanodeIdProto)
                .setStorageInfo(storageInfoProto)
                .setKeys(exportedBlockKeysProto)
                .setSoftwareVersion("2.8.0")
                .build();
    }

    private class HeartbeatTask extends TimerTask {
        private Configuration config;

        public HeartbeatTask(Configuration config) {
            this.config = config;
        }

        @Override
        public void run() {
            List<HdfsProtos.StorageReportProto> reports = new ArrayList<>();

            // TODO - construct storage reports
            
            DatanodeProtocolProtos.HeartbeatRequestProto req = 
                DatanodeProtocolProtos.HeartbeatRequestProto.newBuilder()
                    .setRegistration(buildDatanodeRegistrationProto())
                    .addAllReports(reports)
                    .build();

            // send rpc request
            try {
                byte[] respBuf = rpcClient.send("sendHeartbeat", req);

                // TODO - handle response
                DatanodeProtocolProtos.HeartbeatResponseProto response =
                    DatanodeProtocolProtos.HeartbeatResponseProto.parseFrom(respBuf);
            } catch(Exception e) {
                e.printStackTrace();
                System.err.println("failed to send datanode heartbeat: " + e);
            }
        }
    }
}
