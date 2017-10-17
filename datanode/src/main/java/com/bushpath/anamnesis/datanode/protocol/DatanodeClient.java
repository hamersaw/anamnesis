package com.bushpath.anamnesis.datanode.protocol;

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;

import com.bushpath.anamnesis.protocol.DatanodeProtocol;
import com.bushpath.anamnesis.protocol.Hdfs;
import com.bushpath.anamnesis.protocol.HdfsServer;
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
    private static final String HDFS_VERSION = "2.8.0";
    private static final int LAYOUT_VERSION = 0;
    private static final boolean IS_BLOCK_TOKEN_ENABLED = false;
    private static final long KEY_UPDATE_INTERVAL = 10000, TOKEN_LIFETIME = 30000;
        
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

        this.currentKeyID = 0;
    }

    public void registerDatanode(String ipAddr, String hostName, String datanodeUuid,
            int xferPort, int infoPort, int ipcPort, int namespceID, String clusterID) {
        logger.info("registering datanode");
        
        // construct register request protobuf components
        HdfsProtos.DatanodeIDProto datanodeIDProto = Hdfs.buildDatanodeIDProto(
                ipAddr, hostName, datanodeUuid, xferPort, infoPort, ipcPort);

        HdfsServerProtos.StorageInfoProto storageInfoProto = 
            HdfsServer.buildStorageInfoProto(this.LAYOUT_VERSION, namespceID, 
                clusterID, System.currentTimeMillis());

        HdfsServerProtos.BlockKeyProto currentKey = HdfsServer.buildBlockKeyProto(
            this.currentKeyID, System.currentTimeMillis() + this.TOKEN_LIFETIME);

        List<HdfsServerProtos.BlockKeyProto> allKeys = new ArrayList<>(); // TODO - fill

        HdfsServerProtos.ExportedBlockKeysProto exportedBlockKeysProto =
            HdfsServer.buildExportedBlockKeysProto(this.IS_BLOCK_TOKEN_ENABLED, 
                this.KEY_UPDATE_INTERVAL, this.TOKEN_LIFETIME, currentKey, allKeys);

        DatanodeProtocolProtos.DatanodeRegistrationProto datanodeRegistrationProto = 
            DatanodeProtocol.buildDatanodeRegistrationProto( datanodeIDProto,
                storageInfoProto, exportedBlockKeysProto, this.HDFS_VERSION);

        DatanodeProtocolProtos.RegisterDatanodeRequestProto req =
            DatanodeProtocol.buildRegisterDatanodeRequestProto(datanodeRegistrationProto);

        // send RegisterDatanodeRequestProto
        DatanodeProtocolProtos.RegisterDatanodeResponseProto response =
            this.blockingStub.registerDatanode(req);

        // TODO - handle response
    }

    public void sendHeartbeat(String ipAddr, String hostName, String datanodeUuid,
            int xferPort, int infoPort, int ipcPort, int namespceID, String clusterID) {
        logger.info("sending heartbeat");

        // construct heartbeat request protobuf components
        HdfsProtos.DatanodeIDProto datanodeIDProto = Hdfs.buildDatanodeIDProto(
                ipAddr, hostName, datanodeUuid, xferPort, infoPort, ipcPort);

        HdfsServerProtos.StorageInfoProto storageInfoProto = 
            HdfsServer.buildStorageInfoProto(this.LAYOUT_VERSION, namespceID, 
                clusterID, System.currentTimeMillis());

        HdfsServerProtos.BlockKeyProto currentKey = HdfsServer.buildBlockKeyProto(
            this.currentKeyID, System.currentTimeMillis() + this.TOKEN_LIFETIME);

        List<HdfsServerProtos.BlockKeyProto> allKeys = new ArrayList<>(); // TODO - fill

        HdfsServerProtos.ExportedBlockKeysProto exportedBlockKeysProto =
            HdfsServer.buildExportedBlockKeysProto(this.IS_BLOCK_TOKEN_ENABLED, 
                this.KEY_UPDATE_INTERVAL, this.TOKEN_LIFETIME, currentKey, allKeys);

        DatanodeProtocolProtos.DatanodeRegistrationProto datanodeRegistrationProto = 
            DatanodeProtocol.buildDatanodeRegistrationProto( datanodeIDProto,
                storageInfoProto, exportedBlockKeysProto, this.HDFS_VERSION);

        List<HdfsProtos.StorageReportProto> reports = new ArrayList<>();
        
        DatanodeProtocolProtos.HeartbeatRequestProto req = 
            DatanodeProtocol.buildHeartbeatRequestProto(
                    datanodeRegistrationProto, reports);

        // send HeartbeatRequest
        DatanodeProtocolProtos.HeartbeatResponseProto respnose =
            this.blockingStub.sendHeartbeat(req);

        // TODO - handle response
    }
}
