package com.bushpath.anamnesis.datanode;

import org.apache.hadoop.hdfs.protocol.proto.DatanodeProtocolProtos;

import com.bushpath.anamnesis.datanode.storage.Storage;
import com.bushpath.anamnesis.rpc.RpcClient;

import java.io.IOException;
import java.util.TimerTask;

public class BlockReportTask extends TimerTask {
    private Configuration config;
    private Storage storage;

    public BlockReportTask(Configuration config, Storage storage) {
        this.config = config;
        this.storage = storage;
    }

    @Override
    public void run() {
        RpcClient rpcClient = null;
        try {
            // build request protobuf
            DatanodeProtocolProtos.BlockReportRequestProto req =
                DatanodeProtocolProtos.BlockReportRequestProto.newBuilder()
                    .setRegistration(Main.buildDatanodeRegistrationProto(this.config))
                    .setBlockPoolId(config.poolId)
                    .addReports(this.storage.toStorageBlockReportProto())
                    .build();

            // send rpc request
            rpcClient = new RpcClient(config.namenodeIpAddr,
                config.namenodePort, "datanode",
                "org.apache.hadoop.hdfs.server.protocol.DatanodeProtocol");

            byte[] respBuf = rpcClient.send("blockReport", req);

            // TODO - handle response
        } catch(Exception e) {
            e.printStackTrace();
            System.err.println("failed to send datanode block report: " + e);
        } finally {
            if (rpcClient != null) {
                try {
                    rpcClient.close();
                } catch(IOException e) {
                    System.err.println("failed to close rpc client: " + e);
                }
            }
        }
    }
}
