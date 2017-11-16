package com.bushpath.anamnesis.datanode.rpc;

import com.google.protobuf.Message;
import org.apache.hadoop.hdfs.protocol.proto.ClientDatanodeProtocolProtos;

import com.bushpath.anamnesis.datanode.storage.Storage;

import java.io.DataInputStream;

public class ClientDatanodeService {
    private Storage storage;

    public ClientDatanodeService(Storage storage) {
        this.storage = storage;
    }

    public Message getReplicaVisibleLength(DataInputStream in) throws Exception {
        ClientDatanodeProtocolProtos.GetReplicaVisibleLengthRequestProto req =
            ClientDatanodeProtocolProtos.GetReplicaVisibleLengthRequestProto
                .parseDelimitedFrom(in);

        // retrieve block length
        long blockLength = storage.getBlockLength(req.getBlock().getBlockId());

        // respond to request
        return ClientDatanodeProtocolProtos.GetReplicaVisibleLengthResponseProto
            .newBuilder()
            .setLength(blockLength)
            .build();
    }
}
