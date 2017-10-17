package com.bushpath.anamnesis.protocol;

import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos;

public class ClientNamenodeProtocol {
    public static ClientNamenodeProtocolProtos.MkdirsResponseProto
        buildMkdirsResponseProto(boolean result) {

        return ClientNamenodeProtocolProtos.MkdirsResponseProto.newBuilder()
            .setResult(result)
            .build();
    }
}
