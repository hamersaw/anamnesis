package com.bushpath.anamnesis.datanode.protocol;

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import org.apache.hadoop.hdfs.protocol.proto.DatanodeProtocolProtos;
import org.apache.hadoop.hdfs.protocol.proto.DatanodeProtocolServiceGrpc;
import org.apache.hadoop.hdfs.protocol.proto.HdfsProtos;
import org.apache.hadoop.hdfs.protocol.proto.HdfsServerProtos;

import java.util.ArrayList;
import java.util.List;
import java.util.logging.Logger;

public class DatanodeClient {
    private static final Logger logger = 
        Logger.getLogger(DatanodeClient.class.getName());
        
    // grpc variables
    private final ManagedChannel channel;
    private final 
        DatanodeProtocolServiceGrpc.DatanodeProtocolServiceBlockingStub blockingStub;
    
    // instance variables
    protected int currentKeyID;

    public DatanodeClient(String host, int port) {
        // construct channel and initialize blocking stub
        this.channel = ManagedChannelBuilder.forAddress(host, port)
                        .usePlaintext(true)
                        .build();

        this.blockingStub = DatanodeProtocolServiceGrpc.newBlockingStub(channel);
    }

    public DatanodeProtocolProtos.RegisterDatanodeResponseProto
        registerDatanode(DatanodeProtocolProtos.RegisterDatanodeRequestProto req) {

        return this.blockingStub.registerDatanode(req);
    }

    public DatanodeProtocolProtos.HeartbeatResponseProto
        sendHeartbeat(DatanodeProtocolProtos.HeartbeatRequestProto req) {
        
        return this.blockingStub.sendHeartbeat(req);
    }
}
