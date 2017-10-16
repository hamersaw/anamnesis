package com.bushpath.anamnesis.namenode;

import io.grpc.Server;
import io.grpc.ServerBuilder;

import com.bushpath.anamnesis.namenode.protocol.ClientNamenodeService;
import com.bushpath.anamnesis.namenode.protocol.DatanodeService;
import com.bushpath.anamnesis.namenode.protocol.NamenodeService;

import java.io.IOException;
import java.util.logging.Logger;

public class Main {
    private static final Logger logger = Logger.getLogger(Main.class.getName());

    public static void main(String[] args) {
        try {
            int port = 12289;

            // start server
            NamenodeServer server = new NamenodeServer(port);
            server.start();
            logger.info("server started on port " + port);

            // wait until shutdown command issued
            server.blockUntilShutdown();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static class NamenodeServer {
        private Server server;
        private int port;

        public NamenodeServer(int port) throws IOException {
            this.server = ServerBuilder.forPort(port)
                            .addService(new ClientNamenodeService())
                            .addService(new DatanodeService())
                            .addService(new NamenodeService())
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
                    NamenodeServer.this.stop();
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
