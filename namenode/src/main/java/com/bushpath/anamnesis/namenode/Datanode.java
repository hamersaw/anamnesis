package com.bushpath.anamnesis.namenode;

import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos;
import org.apache.hadoop.hdfs.protocol.proto.HdfsProtos;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.List;

public class Datanode {
    private String ipAddr;
    private String hostname;
    private String datanodeUuid;
    private int xferPort;
    private int infoPort;
    private int ipcPort;
    private long lastUpdate;
    private Map<String,Storage> storages;
    private List<Long> reportedBlockIds;

    public Datanode(String ipAddr, String hostname, String datanodeUuid, 
            int xferPort, int infoPort, int ipcPort, long lastUpdate) {
        this.ipAddr = ipAddr;
        this.hostname = hostname;
        this.datanodeUuid = datanodeUuid;
        this.xferPort = xferPort;
        this.infoPort = infoPort;
        this.ipcPort = ipcPort;
        this.lastUpdate = lastUpdate;
        this.storages = new HashMap<>();
        this.reportedBlockIds = new ArrayList<>();
    }

    public String getIpAddr() {
        return this.ipAddr;
    }

    public String getHostname() {
        return this.hostname;
    }

    public String getDatanodeUuid() {
        return this.datanodeUuid;
    }

    public int getXferPort() {
        return this.xferPort;
    }

    public int getInfoPort() {
        return this.infoPort;
    }

    public int getIpcPort() {
        return this.ipcPort;
    }

    public long getLastUpdate() {
        return this.lastUpdate;
    }

    public void setLastUpdate(long lastUpdate) {
        this.lastUpdate = lastUpdate;
    }

    public void updateStorage(String storageUuid, long capacity, long remaining) {
        if (this.storages.containsKey(storageUuid)) {
            Storage storage = this.storages.get(storageUuid);
            if (storage.capacity != capacity) {
                storage.capacity = capacity;
            }

            storage.remaining = remaining;
        } else {
            this.storages.put(storageUuid, new Storage(capacity, remaining));
        }
    }

    public void setReportedBlockIds(List<Long> reportedBlockIds) {
        this.reportedBlockIds = reportedBlockIds;
    }

    public List<Long> getReportedBlockIds() {
        return this.reportedBlockIds;
    }

    public HdfsProtos.DatanodeIDProto toDatanodeIdProto() {
        return HdfsProtos.DatanodeIDProto.newBuilder()
            .setIpAddr(this.ipAddr)
            .setHostName(this.hostname)
            .setDatanodeUuid(this.datanodeUuid)
            .setXferPort(xferPort)
            .setInfoPort(infoPort)
            .setIpcPort(ipcPort)
            .build();
    }
    
    public HdfsProtos.DatanodeInfoProto toDatanodeInfoProto() {
        return HdfsProtos.DatanodeInfoProto.newBuilder()
            .setId(this.toDatanodeIdProto())
            .setLastUpdate(this.lastUpdate)
            .setLocation("/") // TODO - set to correct location
            .build();
    }

    public ClientNamenodeProtocolProtos.DatanodeStorageReportProto
            toDatanodeStorageReportProto() {
        List<HdfsProtos.StorageReportProto> storageReports = new ArrayList<>();
        for (Map.Entry<String, Storage> entry : this.storages.entrySet()) {
            HdfsProtos.DatanodeStorageProto datanodeStorageProto =
                HdfsProtos.DatanodeStorageProto.newBuilder()
                    .setStorageUuid(entry.getKey())
                    .build();

            Storage storage = entry.getValue();
            HdfsProtos.StorageReportProto storageReport =
                HdfsProtos.StorageReportProto.newBuilder()
                    .setStorageUuid(entry.getKey())
                    .setCapacity(storage.capacity)
                    .setRemaining(storage.remaining)
                    .setStorage(datanodeStorageProto)
                    .build();

            storageReports.add(storageReport);
        }

        return ClientNamenodeProtocolProtos.DatanodeStorageReportProto.newBuilder()
            .setDatanodeInfo(this.toDatanodeInfoProto())
            .addAllStorageReports(storageReports)
            .build();
    }

    private class Storage {
        public long capacity;
        public long remaining;

        public Storage(long capacity, long remaining) {
            this.capacity = capacity;
            this.remaining = remaining;
        }
    }
}
