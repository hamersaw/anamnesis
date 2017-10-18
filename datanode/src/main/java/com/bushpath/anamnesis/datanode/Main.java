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
import java.util.Timer;
import java.util.TimerTask;
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
            
            // start DatanodeClient
            DatanodeClient client = new DatanodeClient(config.namenodeIpAddr,
                config.namenodePort);
            client.registerDatanode(config.ipAddr, config.hostname, config.datanodeUuid,
                config.xferPort, config.infoPort, config.ipcPort, 
                config.namespceId, config.clusterId);

            // start heartbeat timer
            Timer timer = new Timer(true);
            timer.scheduleAtFixedRate(new HeartbeatTask(client, config), 0, 5 * 1000);

            // wait until shutdown command issued
            server.blockUntilShutdown();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static class HeartbeatTask extends TimerTask {
        private DatanodeClient client;
        private Configuration config;

        public HeartbeatTask(DatanodeClient client, Configuration config) {
            this.client = client;
            this.config = config;
        }

        @Override
        public void run() {
            client.sendHeartbeat(this.config.ipAddr, this.config.hostname, 
                this.config.datanodeUuid, this.config.xferPort, this.config.infoPort,
                this.config.ipcPort, this.config.namespceId, this.config.clusterId);
        }
    }
}
