package com.bushpath.anamnesis.protocol;

import com.google.protobuf.ByteString;

import org.apache.hadoop.hdfs.protocol.proto.HdfsProtos;

import java.util.List;

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

    public static HdfsProtos.DirectoryListingProto buildDirectoryListingProto(
            List<HdfsProtos.HdfsFileStatusProto> partialListing, int remainingEntries) {
        
        return HdfsProtos.DirectoryListingProto.newBuilder()
            .addAllPartialListing(partialListing)
            .setRemainingEntries(remainingEntries)
            .build();
    }

    public static HdfsProtos.FsPermissionProto buildFsPermissionProto(int perm) {
        return HdfsProtos.FsPermissionProto.newBuilder()
            .setPerm(perm) // only 16 bits used
            .build();
    }

    public static HdfsProtos.HdfsFileStatusProto buildHdfsFileStatusProto(
            HdfsProtos.HdfsFileStatusProto.FileType fileType, byte[] path,
            long length, HdfsProtos.FsPermissionProto permission, String owner, 
            String group, long modificationTime, long accessTime) {

        return HdfsProtos.HdfsFileStatusProto.newBuilder()
            .setFileType(fileType)
            .setPath(ByteString.copyFrom(path))
            .setLength(length)
            .setPermission(permission)
            .setOwner(owner)
            .setGroup(group)
            .setModificationTime(modificationTime)
            .setAccessTime(accessTime)
            .build();
    }
}
