package com.bushpath.anamnesis.namenode;

import io.grpc.BindableService;
import io.grpc.Server;
import io.grpc.ServerBuilder;

import com.bushpath.anamnesis.GrpcServer;
import com.bushpath.anamnesis.namenode.protocol.ClientNamenodeService;
import com.bushpath.anamnesis.namenode.protocol.DatanodeService;
import com.bushpath.anamnesis.namenode.protocol.NamenodeService;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Logger;

public class Main {
    private static final Logger logger = Logger.getLogger(Main.class.getName());

    public static void main(String[] args) {
        try {
            int port = 12289;

            // initialize managers
            DatanodeManager datanodeManager = new DatanodeManager();

            // start server
            List<BindableService> services = new ArrayList<>();
            services.add(new ClientNamenodeService(datanodeManager));
            services.add(new DatanodeService(datanodeManager));
            services.add(new NamenodeService());
            GrpcServer server = new GrpcServer(port, services);
            server.start();
            logger.info("server started on port " + port);

            // wait until shutdown command issued
            server.blockUntilShutdown();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
