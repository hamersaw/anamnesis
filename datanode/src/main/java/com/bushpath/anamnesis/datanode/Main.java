package com.bushpath.anamnesis.datanode;

import org.apache.hadoop.hdfs.protocol.proto.DatanodeProtocolProtos;
import org.apache.hadoop.hdfs.protocol.proto.HdfsProtos;
import org.apache.hadoop.hdfs.protocol.proto.HdfsServerProtos;

import com.bushpath.anamnesis.ipc.rpc.RpcClient;
import com.bushpath.anamnesis.ipc.rpc.RpcServer;
import com.bushpath.anamnesis.datanode.ipc.datatransfer.DataTransferService;
import com.bushpath.anamnesis.datanode.ipc.rpc.ClientDatanodeService;
import com.bushpath.anamnesis.datanode.storage.JVMStorage;
import com.bushpath.anamnesis.datanode.storage.Storage;

import java.io.DataInputStream;
import java.io.IOException;
import java.net.ServerSocket;
import java.util.ArrayList;
import java.util.List;
import java.util.Timer;

public class Main {
    public static final String softwareVersion = "2.8.0";

    public static void main(String[] args) {
        try {
            // parse configuration file
            if (args.length != 1) {
                System.out.println("USAGE: ./datanode CONFIG_FILE");
                System.exit(1);
            }
            Configuration config = new Configuration(args[0]);

            // initialize storage
            Storage storage;
            switch (config.storage) {
            case "jvm":
                storage = new JVMStorage(config.storageUuid, config.justInTimeInflation);
                break;
            default:
                throw new Exception("Unknown storage type");
            }

            // initialize rpc server
            ServerSocket serverSocket = new ServerSocket(config.ipcPort);
            RpcServer rpcServer = new RpcServer(serverSocket);

            rpcServer.addRpcProtocol(
                "org.apache.hadoop.hdfs.protocol.ClientDatanodeProtocol",
                new ClientDatanodeService(storage));

            rpcServer.start();

            // start data transfer service
            new Thread(new DataTransferService(config.xferPort, storage))
                .start();

            // send registration
            try {
                DatanodeProtocolProtos.RegisterDatanodeRequestProto req =
                    DatanodeProtocolProtos.RegisterDatanodeRequestProto.newBuilder()
                        .setRegistration(buildDatanodeRegistrationProto(config))
                        .build();

                RpcClient rpcClient = new RpcClient(config.namenodeIpAddr,
                    config.namenodePort, "datanode",
                    "org.apache.hadoop.hdfs.server.protocol.DatanodeProtocol");

                DataInputStream in = rpcClient.send("registerDatanode", req);

                // TODO - handle response
                DatanodeProtocolProtos.RegisterDatanodeResponseProto response =
                    DatanodeProtocolProtos.RegisterDatanodeResponseProto
                        .parseDelimitedFrom(in);

                rpcClient.close();
            } catch(Exception e) {
                System.err.println("failed to register datanode: " + e);
                System.exit(1);
            }
            
            // start  timer tasks
            Timer timer = new Timer(true);
            long blockReportIntervalMs = config.blockReportInterval * 1000;
            long heartbeatIntervalMs = config.heartbeatInterval * 1000;
            long memoryEvictionIntervalMs = config.memoryEvictionInterval * 1000;
            timer.scheduleAtFixedRate(new BlockReportTask(config, storage),
                blockReportIntervalMs, blockReportIntervalMs);
            timer.scheduleAtFixedRate(new HeartbeatTask(config, storage),
                heartbeatIntervalMs, heartbeatIntervalMs);
            timer.scheduleAtFixedRate(new MemoryEvictionTask(config, storage),
                memoryEvictionIntervalMs, memoryEvictionIntervalMs);

            // wait until rpc server shuts down
            rpcServer.join();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static DatanodeProtocolProtos.DatanodeRegistrationProto 
        buildDatanodeRegistrationProto(Configuration config) {

        HdfsProtos.DatanodeIDProto datanodeIdProto = 
            HdfsProtos.DatanodeIDProto.newBuilder()
                .setIpAddr(config.ipAddr)
                .setHostName(config.hostname)
                .setDatanodeUuid(config.datanodeUuid)
                .setXferPort(config.xferPort)
                .setInfoPort(config.infoPort)
                .setIpcPort(config.ipcPort)
                .build();

        HdfsServerProtos.StorageInfoProto storageInfoProto = 
            HdfsServerProtos.StorageInfoProto.newBuilder()
                .setLayoutVersion(0)
                .setNamespceID(config.namespceId)
                .setClusterID(config.clusterId)
                .setCTime(System.currentTimeMillis())
                .build();

        // TODO - build with storage (get key information)
        HdfsServerProtos.BlockKeyProto currentKey =
            HdfsServerProtos.BlockKeyProto.newBuilder()
                .setKeyId(0)
                .setExpiryDate(-1)
                .build();

        // storage is not persistent - will never have any keys
        List<HdfsServerProtos.BlockKeyProto> allKeys = new ArrayList<>();

        HdfsServerProtos.ExportedBlockKeysProto exportedBlockKeysProto =
            HdfsServerProtos.ExportedBlockKeysProto.newBuilder()
                .setIsBlockTokenEnabled(false)
                .setKeyUpdateInterval(-1)
                .setTokenLifeTime(-1)
                .setCurrentKey(currentKey)
                .addAllAllKeys(allKeys)
                .build();

        return DatanodeProtocolProtos.DatanodeRegistrationProto.newBuilder()
                .setDatanodeID(datanodeIdProto)
                .setStorageInfo(storageInfoProto)
                .setKeys(exportedBlockKeysProto)
                .setSoftwareVersion(softwareVersion)
                .build();
    }
}
