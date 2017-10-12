package com.bushpath.phoenix.datanode;

import io.grpc.Server;
import io.grpc.ServerBuilder;

import java.io.IOException;

public class Main {
    public static void main(String[] args) {
        try {
            new DatanodeServer().start();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static class DatanodeServer {
        private Server server;

        private void start() throws IOException {
            server = ServerBuilder.forPort(12289)
                .addService(new DatanodeService())
                .addService(new ClientDatanodeService())
                .build()
                .start();

            Runtime.getRuntime().addShutdownHook(new Thread() {
                @Override
                public void run() {
                    System.err.println("Shutting down GRPC server.");
                    DatanodeServer.this.stop();
                    System.err.println("Shutdown complete");
                }
            });
        }

        private void stop() {
            if (server != null) {
                server.shutdown();
            }
        }
    }
}
