package com.bushpath.anamnesis.namenode;

import org.apache.hadoop.hdfs.protocol.proto.HdfsProtos;

public class Datanode {
    private String ipAddr, hostname, datanodeUuid;
    private int xferPort, infoPort, ipcPort;
    private long lastUpdate;

    public Datanode(String ipAddr, String hostname, String datanodeUuid, 
            int xferPort, int infoPort, int ipcPort, long lastUpdate) {
        this.ipAddr = ipAddr;
        this.hostname = hostname;
        this.datanodeUuid = datanodeUuid;
        this.xferPort = xferPort;
        this.infoPort = infoPort;
        this.ipcPort = ipcPort;
        this.lastUpdate = lastUpdate;
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
            .build();
    }
}
