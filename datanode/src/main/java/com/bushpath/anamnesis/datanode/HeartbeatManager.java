package com.bushpath.anamnesis.datanode;

import com.bushpath.anamnesis.datanode.protocol.DatanodeClient;

import java.util.Timer;
import java.util.TimerTask;

public class HeartbeatManager {
    private DatanodeClient client;
    private Configuration config;

    public HeartbeatManager(DatanodeClient client, Configuration config) {
        this.client = client;
        this.config = config;

        // send initial register message
        client.registerDatanode(config.ipAddr, config.hostname, config.datanodeUuid,
            config.xferPort, config.infoPort, config.ipcPort, 
            config.namespceId, config.clusterId);

        // start heartbeat timer
        Timer timer = new Timer(true);
        timer.scheduleAtFixedRate(new HeartbeatTask(client, config), 0, 5 * 1000);
    }

    private static class HeartbeatTask extends TimerTask {
        private DatanodeClient client;
        private Configuration config;

        public HeartbeatTask(DatanodeClient client, Configuration config) {
            this.client = client;
            this.config = config;
        }

        @Override
        public void run() {
            client.sendHeartbeat(this.config.ipAddr, this.config.hostname, 
                this.config.datanodeUuid, this.config.xferPort, this.config.infoPort,
                this.config.ipcPort, this.config.namespceId, this.config.clusterId);
        }
    }
}
