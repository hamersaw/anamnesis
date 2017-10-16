package com.bushpath.anamnesis.protocol;

import org.apache.hadoop.hdfs.protocol.proto.HdfsProtos;

public class Hdfs {
    public static HdfsProtos.DatanodeIDProto buildDatanodeIDProto(String ipAddr, 
            String hostName, String datanodeUuid, int xferPort, 
            int infoPort, int ipcPort) {

        return HdfsProtos.DatanodeIDProto.newBuilder()
            .setIpAddr(ipAddr)
            .setHostName(hostName)
            .setDatanodeUuid(datanodeUuid)
            .setXferPort(xferPort)
            .setInfoPort(infoPort)
            .setIpcPort(ipcPort)
            .build();
    }
}
