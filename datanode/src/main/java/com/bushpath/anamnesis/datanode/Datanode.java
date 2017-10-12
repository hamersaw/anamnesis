package com.bushpath.anamnesis.datanode;

import io.grpc.Server;
import io.grpc.ServerBuilder;

import java.io.IOException;

public class Datanode {
    public class DatanodeServer {
        public Server server;

        public void start() throws IOException {
            server = ServerBuilder.forPort(12289)
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

        public void stop() {
            if (server != null) {
                server.shutdown();
            }
        }
    }
}
