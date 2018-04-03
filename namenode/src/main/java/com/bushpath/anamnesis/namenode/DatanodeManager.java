package com.bushpath.anamnesis.namenode;

import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import org.apache.hadoop.hdfs.protocol.proto.DatanodeProtocolProtos;
import org.apache.hadoop.hdfs.protocol.proto.HdfsProtos;

import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;

public class DatanodeManager {
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
 
            // ensure datanode exists
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

    public void updateDatanodeStorage(String datanodeUuid, String storageUuid,
            long capacity, long remaining) throws Exception {
        this.lock.writeLock().lock();
        try {
            // ensure datanodeuuid is not empty
            if (datanodeUuid.isEmpty()) {
                throw new StatusRuntimeException(Status.INVALID_ARGUMENT
                    .withDescription("unable to update datanode storage " +
                        "with empty datanode uuid"));
            }
 
            // ensure datanode exists
            if (!this.datanodes.containsKey(datanodeUuid)) {
                throw new StatusRuntimeException(Status.NOT_FOUND
                    .withDescription("'" + datanodeUuid + "' does not exist"));
            }

            // update datanode
            Datanode datanode = this.datanodes.get(datanodeUuid);
            datanode.updateStorage(storageUuid, capacity, remaining);
        } finally {
            this.lock.writeLock().unlock();
        }
    }

    public boolean contains(String datanodeUuid) {
        return this.datanodes.containsKey(datanodeUuid);
    }

    public Datanode get(String datanodeUuid) {
        return this.datanodes.get(datanodeUuid);
    }

    public Collection<Datanode> getDatanodes() {
        return this.datanodes.values();
    }

    public Datanode getRandom() {
        int rand = this.random.nextInt(this.datanodes.size());
        for (Datanode datanode: this.datanodes.values()) {
            if (rand == 0) {
                return datanode;
            }

            rand -= 1;
        }

        return null;
    }
}
