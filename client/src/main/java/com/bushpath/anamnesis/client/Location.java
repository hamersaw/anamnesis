package com.bushpath.anamnesis.client;

import org.apache.hadoop.hdfs.protocol.proto.HdfsProtos;

public class Location {
    private String ipAddr;
    private int port;

    public Location(String ipAddr, int port) {
        this.ipAddr = ipAddr;
        this.port = port;
    }

    public String getIpAddr() {
        return this.ipAddr;
    }

    public int getPort() {
        return port;
    }

    public static Location parseFrom(HdfsProtos.DatanodeIDProto datanodeIDProto) {
        return new Location(datanodeIDProto.getIpAddr(), datanodeIDProto.getXferPort());
    }
}
