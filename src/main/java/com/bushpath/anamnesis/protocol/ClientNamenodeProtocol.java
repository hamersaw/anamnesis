package com.bushpath.anamnesis.protocol;

import com.google.protobuf.ByteString;

import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos;
import org.apache.hadoop.hdfs.protocol.proto.HdfsProtos;

public class ClientNamenodeProtocol {
    public static ClientNamenodeProtocolProtos.GetListingRequestProto
        buildGetListingRequestProto(String src, byte[] startAfter, 
            boolean needLocation) {

        return ClientNamenodeProtocolProtos.GetListingRequestProto.newBuilder()
            .setSrc(src)
            .setStartAfter(ByteString.copyFrom(startAfter))
            .setNeedLocation(needLocation)
            .build();
    }

    public static ClientNamenodeProtocolProtos.GetListingResponseProto
        buildGetListingResponseProto(HdfsProtos.DirectoryListingProto dirList) {

        return ClientNamenodeProtocolProtos.GetListingResponseProto.newBuilder()
            .setDirList(dirList)
            .build();
    }

    public static ClientNamenodeProtocolProtos.MkdirsRequestProto
        buildMkdirsRequestProto(String src, HdfsProtos.FsPermissionProto masked, 
            boolean createParent) {

        return ClientNamenodeProtocolProtos.MkdirsRequestProto.newBuilder()
            .setSrc(src)
            .setMasked(masked)
            .setCreateParent(createParent)
            .build();
    }

    public static ClientNamenodeProtocolProtos.MkdirsResponseProto
        buildMkdirsResponseProto(boolean result) {

        return ClientNamenodeProtocolProtos.MkdirsResponseProto.newBuilder()
            .setResult(result)
            .build();
    }
}
