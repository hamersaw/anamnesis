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
            // start server
            int serverPort = 15605;
            List<BindableService> services = new ArrayList<>();
            services.add(new ClientDatanodeService());
            GrpcServer server = new GrpcServer(15605, services);
            server.start();
            logger.info("server started on port " + serverPort);
            
            // start DatanodeClient
            String namenodeIpAddr = "localhost", ipAddr = "localhost", 
                hostName = "foo", datanodeUuid = "", clusterID = "";
            int namenodePort = 12289, xferPort = -1, infoPort = -1, ipcPort = -1,
                namespceID = 0;

            DatanodeClient client = new DatanodeClient(namenodeIpAddr, namenodePort);
            client.registerDatanode(ipAddr, hostName, datanodeUuid, xferPort, 
                infoPort, ipcPort, namespceID, clusterID);

            // start heartbeat timer
            Timer timer = new Timer(true);
            timer.scheduleAtFixedRate(new HeartbeatTask(client), 0, 5 * 1000);

            // wait until shutdown command issued
            server.blockUntilShutdown();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static class HeartbeatTask extends TimerTask {
        private DatanodeClient client;

        public HeartbeatTask(DatanodeClient client) {
            this.client = client;
        }

        @Override
        public void run() {
            String ipAddr = "localhost", hostName = "foo", datanodeUuid = "", 
                clusterID = "";
            int xferPort = -1, infoPort = -1, ipcPort = -1, namespceID = 0;

            client.sendHeartbeat(ipAddr, hostName, datanodeUuid, xferPort, 
                infoPort, ipcPort, namespceID, clusterID);
        }
    }
}
