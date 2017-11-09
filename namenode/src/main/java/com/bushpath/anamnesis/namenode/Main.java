package com.bushpath.anamnesis.namenode;

import io.grpc.BindableService;
import io.grpc.Server;
import io.grpc.ServerBuilder;

import com.bushpath.anamnesis.rpc.RpcServer;
import com.bushpath.anamnesis.namenode.rpc.ClientNamenodeService;
import com.bushpath.anamnesis.namenode.rpc.DatanodeService;

import java.io.IOException;
import java.net.ServerSocket;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Logger;

public class Main {
    private static final Logger logger = Logger.getLogger(Main.class.getName());

    public static void main(String[] args) {
        try {
            // parse configuration file
            if (args.length != 1) {
                System.out.println("USAGE: ./namenode CONFIG_FILE");
                System.exit(1);
            }
            Configuration config = new Configuration(args[0]);

            // initialize managers
            DatanodeManager datanodeManager = new DatanodeManager();
            NameSystem nameSystem = new NameSystem();
            BlockManager blockManager = new BlockManager(datanodeManager, nameSystem);

            // start rpc server
            ServerSocket serverSocket = new ServerSocket(config.port);
            RpcServer rpcServer = new RpcServer(serverSocket);
            rpcServer.registerRpcHandler(
                "org.apache.hadoop.hdfs.protocol.ClientProtocol",
                new ClientNamenodeService(nameSystem, blockManager));
            rpcServer.registerRpcHandler(
                "org.apache.hadoop.hdfs.server.protocol.DatanodeProtocol",
                new DatanodeService(datanodeManager));
            rpcServer.start();

            // wait until rpcServer shuts down
            rpcServer.join();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
