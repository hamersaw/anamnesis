package com.bushpath.anamnesis.namenode;

import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import org.apache.hadoop.hdfs.protocol.proto.DatanodeProtocolProtos;
import org.apache.hadoop.hdfs.protocol.proto.HdfsProtos;

import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.logging.Logger;

public class DatanodeManager {
    private static final Logger logger =
        Logger.getLogger(DatanodeManager.class.getName());

    protected ReadWriteLock lock;
    protected Random random;

    protected Map<String, Datanode> datanodes;

    public DatanodeManager() {
        this.lock = new ReentrantReadWriteLock();
        this.random = new Random();
        this.datanodes = new HashMap<>();
    }

    public void registerDatanode(String ipAddr, String hostname, String datanodeUuid, 
            int xferPort, int infoPort, int ipcPort, long lastUpdate) throws Exception {
        this.lock.writeLock().lock();
        try {
            logger.info("registering node '" + datanodeUuid + "'");

            // ensure datanodeuuid is not empty
            if (datanodeUuid.isEmpty()) {
                throw new StatusRuntimeException(Status.INVALID_ARGUMENT
                    .withDescription("unable to register empty datanode uuid"));
            }
 
            // check if datanode already exists
            if (this.datanodes.containsKey(datanodeUuid)) {
                throw new StatusRuntimeException(Status.ALREADY_EXISTS
                    .withDescription("'" + datanodeUuid + "' already exists"));
            }

            // store new datanode
            Datanode datanode = new Datanode(ipAddr, hostname, datanodeUuid, 
                xferPort, infoPort, ipcPort, lastUpdate);
            this.datanodes.put(datanodeUuid, datanode);
        } finally {
            this.lock.writeLock().unlock();
        }
    }

    public void updateDatanode(String datanodeUuid, long lastUpdate) throws Exception {
        this.lock.writeLock().lock();
        try {
            // ensure datanodeuuid is not empty
            if (datanodeUuid.isEmpty()) {
                throw new StatusRuntimeException(Status.INVALID_ARGUMENT
                    .withDescription("unable to register empty datanode uuid"));
            }
 
            // enxure datanode exists
            if (!this.datanodes.containsKey(datanodeUuid)) {
                throw new StatusRuntimeException(Status.NOT_FOUND
                    .withDescription("'" + datanodeUuid + "' does not exist"));
            }

            // update datanode
            Datanode datanode = this.datanodes.get(datanodeUuid);
            datanode.setLastUpdate(lastUpdate);
        } finally {
            this.lock.writeLock().unlock();
        }
    }

    public Datanode chooseBlockLocation(long blockSize,
            List<String> favoredNodes) throws Exception {
        if (this.datanodes.size() == 0) {
            throw new StatusRuntimeException(Status.UNAVAILABLE
                .withDescription("no datanodes are registered"));
        }

        // try all favored nodes
        if (favoredNodes != null) {
            for (String favoredNode: favoredNodes) {
                if (this.datanodes.containsKey(favoredNode)) {
                    return this.datanodes.get(favoredNode);
                }
            }
        }

        // return random node
        Object[] array = this.datanodes.values().toArray();
        return (Datanode) array[this.random.nextInt(array.length)];
    }
}
