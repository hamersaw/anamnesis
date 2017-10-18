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

/*message LocatedBlockProto {
  required ExtendedBlockProto b  = 1;
  required uint64 offset = 2;           // offset of first byte of block in the file
  repeated DatanodeInfoProto locs = 3;  // Locations ordered by proximity to client ip
  required bool corrupt = 4;            // true if all replicas of a block are corrupt, else false
                                        // If block has few corrupt replicas, they are filtered and 
                                        // their locations are not part of this object

  required hadoop.common.TokenProto blockToken = 5;
  repeated bool isCached = 6 [packed=true]; // if a location in locs is cached
  repeated StorageTypeProto storageTypes = 7;
  repeated string storageIDs = 8;
}
message ExtendedBlockProto {
  required string poolId = 1;   // Block pool id - gloablly unique across clusters
  required uint64 blockId = 2;  // the local id within a pool
  required uint64 generationStamp = 3;
  optional uint64 numBytes = 4 [default = 0];  // len does not belong in ebid 
                                               // here for historical reasons
}*/

/*
message DatanodeInfoProto {
  required DatanodeIDProto id = 1;
  optional uint64 capacity = 2 [default = 0];
  optional uint64 dfsUsed = 3 [default = 0];
  optional uint64 remaining = 4 [default = 0];
  optional uint64 blockPoolUsed = 5 [default = 0];
  optional uint64 lastUpdate = 6 [default = 0];
  optional uint32 xceiverCount = 7 [default = 0];
  optional string location = 8;
  optional uint64 nonDfsUsed = 9;
  enum AdminState {
    NORMAL = 0;
    DECOMMISSION_INPROGRESS = 1;
    DECOMMISSIONED = 2;
  }

  optional AdminState adminState = 10 [default = NORMAL];
  optional uint64 cacheCapacity = 11 [default = 0];
  optional uint64 cacheUsed = 12 [default = 0];
  optional uint64 lastUpdateMonotonic = 13 [default = 0];
  optional string upgradeDomain = 14;
}*/
}
