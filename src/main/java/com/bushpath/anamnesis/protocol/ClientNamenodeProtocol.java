package com.bushpath.anamnesis.protocol;

import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos;
import org.apache.hadoop.hdfs.protocol.proto.HdfsProtos;

public class ClientNamenodeProtocol {
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
