package com.bushpath.anamnesis.namenode;

import io.grpc.Server;
import io.grpc.ServerBuilder;

import com.bushpath.anamnesis.namenode.protocol.ClientNamenodeService;
import com.bushpath.anamnesis.namenode.protocol.DatanodeService;
import com.bushpath.anamnesis.namenode.protocol.NamenodeService;

import java.io.IOException;

public class Namenode {
    private static class NamenodeServer {
        private Server server;

        private void start() throws IOException {
            server = ServerBuilder.forPort(12289)
                .addService(new ClientNamenodeService())
                .addService(new DatanodeService())
                .addService(new NamenodeService())
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
