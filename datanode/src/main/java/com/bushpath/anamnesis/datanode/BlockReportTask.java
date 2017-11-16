package com.bushpath.anamnesis.datanode;

import java.util.TimerTask;

public class BlockReportTask extends TimerTask {
    private Configuration config;

    public BlockReportTask(Configuration config) {
        this.config = config;
    }

    @Override
    public void run() {
        System.out.println("TODO - send block report");
        try {
            /*RpcClient rpcClient = new RpcClient(config.namenodeIpAddr,
                config.namenodePort, "datanode",
                "org.apache.hadoop.hdfs.server.protocol.DatanodeProtocol");

            byte[] respBuf = rpcClient.send("sendHeartbeat", req);*/
        } catch(Exception e) {
            e.printStackTrace();
            System.err.println("failed to send datanode block report: " + e);
        }
    }
}
