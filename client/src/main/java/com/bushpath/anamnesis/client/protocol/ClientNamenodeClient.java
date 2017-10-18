package com.bushpath.anamnesis.client.protocol;

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;

import com.bushpath.anamnesis.protocol.ClientNamenodeProtocol;
import com.bushpath.anamnesis.protocol.Hdfs;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolGrpc;
import org.apache.hadoop.hdfs.protocol.proto.HdfsProtos;

import java.util.ArrayList;
import java.util.List;
import java.util.logging.Logger;

public class ClientNamenodeClient {
    private static final Logger logger = 
        Logger.getLogger(ClientNamenodeClient.class.getName());

    // grpc variables
    private final ManagedChannel channel;
    private final 
        ClientNamenodeProtocolGrpc.ClientNamenodeProtocolBlockingStub 
        blockingStub;
    
    public ClientNamenodeClient(String host, int port) {
        // construct channel and initialize blocking stub
        this.channel = ManagedChannelBuilder.forAddress(host, port)
                        .usePlaintext(true)
                        .build();

        this.blockingStub = ClientNamenodeProtocolGrpc.newBlockingStub(channel);
    }

    public void addBlock() {

    }

    public ClientNamenodeProtocolProtos.CreateResponseProto 
        create(ClientNamenodeProtocolProtos.CreateRequestProto req) {

        return this.blockingStub.create(req);
    }

    public ClientNamenodeProtocolProtos.GetListingResponseProto 
        getListing(ClientNamenodeProtocolProtos.GetListingRequestProto req) {
 
        return this.blockingStub.getListing(req);
    }

    public ClientNamenodeProtocolProtos.MkdirsResponseProto
        mkdirs(ClientNamenodeProtocolProtos.MkdirsRequestProto req) {
            
        return this.blockingStub.mkdirs(req);
    }
}
