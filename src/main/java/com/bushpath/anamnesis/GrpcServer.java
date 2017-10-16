package com.bushpath.anamnesis;

import io.grpc.BindableService;
import io.grpc.Server;
import io.grpc.ServerBuilder;

import java.io.IOException;
import java.util.List;

public class GrpcServer {
    private Server server;
    private int port;

    public GrpcServer(int port, List<BindableService> services) throws IOException {
        ServerBuilder serverBuilder = ServerBuilder.forPort(port);
        for (BindableService service: services) {
            serverBuilder.addService(service);
        }

        this.server = serverBuilder.build();
        this.port = port;
    }

    // start server with services
    public void start() throws IOException {
        this.server.start();

        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                System.err.println("Shutting down GRPC server.");
                GrpcServer.this.stop();
                System.err.println("Shutdown complete");
            }
        });
    }

    // stop server
    public void stop() {
        if (this.server != null) {
            this.server.shutdown();
        }
    }

    // keep server running until implicitly shutdown
    public void blockUntilShutdown() throws InterruptedException {
        if (this.server != null) {
            this.server.awaitTermination();
        }
    }
}
