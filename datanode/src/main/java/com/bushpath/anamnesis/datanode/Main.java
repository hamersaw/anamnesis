package com.bushpath.anamnesis.datanode;

import com.bushpath.anamnesis.rpc.RpcServer;
import com.bushpath.anamnesis.datanode.rpc.ClientDatanodeService;
import com.bushpath.anamnesis.datanode.storage.JVMStorage;
import com.bushpath.anamnesis.datanode.storage.Storage;
import com.bushpath.anamnesis.datanode.storage.TmpfsStorage;

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
                System.out.println("USAGE: ./datanode CONFIG_FILE");
                System.exit(1);
            }
            Configuration config = new Configuration(args[0]);

            // initialize storage
            Storage storage;
            switch (config.storage) {
            case "jvm":
                storage = new JVMStorage();
                break;
            case "tmpfs":
                storage = new TmpfsStorage();
                break;
            default:
                throw new Exception("Unknown storage type");
            }

            // initialize rpc server
            ServerSocket serverSocket = new ServerSocket(config.ipcPort);
            RpcServer rpcServer = new RpcServer(serverSocket);
            rpcServer.registerRpcHandler(
                "org.apache.hadoop.hdfs.protocol.ClientDatanodeProtocol",
                new ClientDatanodeService());
            // TODO - register client datanode service
            rpcServer.start();

            // start xfer service
            new Thread(new XferService(config.xferPort, storage)).start();            
            
            // start HeartbeatManager
            HeartbeatManager heartbeatManager = new HeartbeatManager(config);

            // wait until rpc server shuts down
            rpcServer.join();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
