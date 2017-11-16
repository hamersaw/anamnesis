package com.bushpath.anamnesis.datanode.rpc;

import com.google.protobuf.Message;
import org.apache.hadoop.hdfs.protocol.proto.ClientDatanodeProtocolProtos;

import java.io.DataInputStream;

public class ClientDatanodeService {
    public Message getReplicaVisibleLength(DataInputStream in) throws Exception {
        ClientDatanodeProtocolProtos.GetReplicaVisibleLengthRequestProto req =
            ClientDatanodeProtocolProtos.GetReplicaVisibleLengthRequestProto
                .parseDelimitedFrom(in);

        // TODO - retrieve block length

        // respond to request
        return ClientDatanodeProtocolProtos.GetReplicaVisibleLengthResponseProto
            .newBuilder()
            .setLength(13l)
            .build();
    }
}
