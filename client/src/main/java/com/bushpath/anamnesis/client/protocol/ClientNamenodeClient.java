package com.bushpath.anamnesis.client.protocol;

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;

import com.bushpath.anamnesis.protocol.ClientNamenodeProtocol;
import com.bushpath.anamnesis.protocol.Hdfs;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolGrpc;
import org.apache.hadoop.hdfs.protocol.proto.HdfsProtos;

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

    public void mkdir(String path, int perm, boolean createParent) {
        // construct protobuf components
        HdfsProtos.FsPermissionProto fsPermissionProto = 
            Hdfs.buildFsPermissionProto(perm);

        ClientNamenodeProtocolProtos.MkdirsRequestProto req = 
            ClientNamenodeProtocol.buildMkdirsRequestProto(path, 
                fsPermissionProto, createParent);

        // send MkdirsRequestProto
        ClientNamenodeProtocolProtos.MkdirsResponseProto response =
            this.blockingStub.mkdirs(req);

        // TODO - handle response
    }
}
