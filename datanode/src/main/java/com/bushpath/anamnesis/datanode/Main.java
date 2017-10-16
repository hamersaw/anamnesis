package com.bushpath.anamnesis.datanode;

import io.grpc.Server;
import io.grpc.ServerBuilder;

import com.bushpath.anamnesis.datanode.protocol.ClientDatanodeService;
import com.bushpath.anamnesis.datanode.protocol.DataNodeClient;

import java.io.IOException;
import java.util.logging.Logger;

public class Main {
    private static final Logger logger = Logger.getLogger(Main.class.getName());

    public static void main(String[] args) {
        try {
            // start server
            int serverPort = 15605;
            DatanodeServer server = new DatanodeServer(serverPort);
            server.start();
            logger.info("server started on port " + serverPort);
            
            // start DataNodeClient
            String namenodeIpAddr = "localhost", ipAddr = "localhost", 
                hostName = "foo", datanodeUuid = "", clusterID = "";
            int namenodePort = 12289, xferPort = -1, infoPort = -1, ipcPort = -1,
                namespceID = 0;

            DataNodeClient client = new DataNodeClient(namenodeIpAddr, namenodePort);
            client.registerDatanode(ipAddr, hostName, datanodeUuid, xferPort, 
                infoPort, ipcPort, namespceID, clusterID);

            // wait until shutdown command issued
            server.blockUntilShutdown();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static class DatanodeServer {
        private Server server;
        private int port;

        public DatanodeServer(int port) throws IOException {
            this.server = ServerBuilder.forPort(port)
                            .addService(new ClientDatanodeService())
                            .build();

            this.port = port;
        }

        // start server with services
        private void start() throws IOException {
            this.server.start();

            Runtime.getRuntime().addShutdownHook(new Thread() {
                @Override
                public void run() {
                    System.err.println("Shutting down GRPC server.");
                    DatanodeServer.this.stop();
                    System.err.println("Shutdown complete");
                }
            });
        }

        // stop server
        private void stop() {
            if (this.server != null) {
                this.server.shutdown();
            }
        }

        // keep server running until implicitly shutdown
        private void blockUntilShutdown() throws InterruptedException {
            if (this.server != null) {
                this.server.awaitTermination();
            }
        }
    }
}
