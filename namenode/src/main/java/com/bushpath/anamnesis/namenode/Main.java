package com.bushpath.anamnesis.namenode;

import io.grpc.BindableService;
import io.grpc.Server;
import io.grpc.ServerBuilder;

//import com.bushpath.anamnesis.GrpcServer;
import com.bushpath.anamnesis.rpc.RpcServer;
//import com.bushpath.anamnesis.namenode.protocol.ClientNamenodeService;
//import com.bushpath.anamnesis.namenode.protocol.DatanodeService;
//import com.bushpath.anamnesis.namenode.protocol.NamenodeService;
import com.bushpath.anamnesis.namenode.rpc.ClientNamenodeService;

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

            // start server
            /*List<BindableService> services = new ArrayList<>();
            services.add(new ClientNamenodeService(blockManager,
                datanodeManager, nameSystem));
            services.add(new DatanodeService(datanodeManager));
            services.add(new NamenodeService());
            GrpcServer server = new GrpcServer(config.port, services);
            server.start();
            logger.info("server started on port " + config.port);

            // wait until shutdown command issued
            server.blockUntilShutdown();*/

            // start rpc server
            ServerSocket serverSocket = new ServerSocket(config.port);
            RpcServer rpcServer = new RpcServer(serverSocket);
            rpcServer.registerRpcHandler(
                "org.apache.hadoop.hdfs.protocol.ClientProtocol",
                new ClientNamenodeService(nameSystem));
            rpcServer.start();

            // wait until rpcServer shuts down
            rpcServer.join();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
