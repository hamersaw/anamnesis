package com.bushpath.anamnesis.datanode;

import org.apache.hadoop.hdfs.protocol.proto.DatanodeProtocolProtos;
import org.apache.hadoop.hdfs.protocol.proto.HdfsProtos;
import org.apache.hadoop.hdfs.protocol.proto.HdfsServerProtos;

import com.bushpath.anamnesis.datanode.protocol.DatanodeClient;

import java.util.ArrayList;
import java.util.List;
import java.util.Timer;
import java.util.TimerTask;

public class HeartbeatManager {
    private DatanodeClient client;
    private Configuration config;
    private HdfsProtos.DatanodeIDProto datanodeIdProto;

    public HeartbeatManager(DatanodeClient client, Configuration config) {
        this.client = client;
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

        DatanodeProtocolProtos.RegisterDatanodeResponseProto response =
            this.client.registerDatanode(req);

        // TODO - handle response

        // start heartbeat timer
        Timer timer = new Timer(true);
        timer.scheduleAtFixedRate(new HeartbeatTask(client, config), 0, 5 * 1000);
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
        private DatanodeClient client;
        private Configuration config;

        public HeartbeatTask(DatanodeClient client, Configuration config) {
            this.client = client;
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

            // send HeartbeatRequest
            DatanodeProtocolProtos.HeartbeatResponseProto response =
                this.client.sendHeartbeat(req);

            // TODO - handle response
        }
    }
}
