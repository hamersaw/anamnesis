package com.bushpath.phoenix.namenode;

import io.grpc.Server;
import io.grpc.ServerBuilder;

import java.io.IOException;

public class Main {
    public static void main(String[] args) {
        try {
            new NamenodeServer().start();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static class NamenodeServer {
        private Server server;

        private void start() throws IOException {
            server = ServerBuilder.forPort(12289)
                .addService(new NamenodeService())
                .addService(new ClientNamenodeService())
                .build()
                .start();

            Runtime.getRuntime().addShutdownHook(new Thread() {
                @Override
                public void run() {
                    System.err.println("Shutting down GRPC server.");
                    NamenodeServer.this.stop();
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
