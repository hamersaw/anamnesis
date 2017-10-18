package com.bushpath.anamnesis.protocol;

import com.google.protobuf.ByteString;

import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos;
import org.apache.hadoop.hdfs.protocol.proto.HdfsProtos;

import java.util.List;

public class ClientNamenodeProtocol {
    public static ClientNamenodeProtocolProtos.CreateRequestProto
        buildCreateRequestProto(String src, HdfsProtos.FsPermissionProto masked, 
            String clientName, int createFlag, boolean createParent, int replication,
            long blockSize, 
            List<HdfsProtos.CryptoProtocolVersionProto> cryptoProtocolVersion) {

        return ClientNamenodeProtocolProtos.CreateRequestProto.newBuilder()
            .setSrc(src)
            .setMasked(masked)
            .setClientName(clientName)
            .setCreateFlag(createFlag)
            .setCreateParent(createParent)
            .setReplication(replication)
            .setBlockSize(blockSize)
            .addAllCryptoProtocolVersion(cryptoProtocolVersion)
            .build();
    }

    public static ClientNamenodeProtocolProtos.CreateResponseProto
        buildCreateResponseProto() {

        return ClientNamenodeProtocolProtos.CreateResponseProto.newBuilder()
            .build();
    }

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
