package com.bushpath.anamnesis.datanode;

import io.grpc.BindableService;
import io.grpc.Server;
import io.grpc.ServerBuilder;

import com.bushpath.anamnesis.GrpcServer;
import com.bushpath.anamnesis.datanode.protocol.ClientDatanodeService;
import com.bushpath.anamnesis.datanode.protocol.DatanodeClient;

import java.io.IOException;
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

            // start server
            List<BindableService> services = new ArrayList<>();
            services.add(new ClientDatanodeService());
            GrpcServer server = new GrpcServer(config.ipcPort, services);
            server.start();
            logger.info("server started on port " + config.ipcPort);

            // start xfer service
            new Thread(new XferService(config.xferPort)).start();            
            
            // start HeartbeatManager
            DatanodeClient client = new DatanodeClient(config.namenodeIpAddr,
                config.namenodePort);

            HeartbeatManager heartbeatManager = new HeartbeatManager(client, config);

            // wait until shutdown command issued
            server.blockUntilShutdown();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
