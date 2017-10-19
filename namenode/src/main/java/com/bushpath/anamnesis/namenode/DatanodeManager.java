package com.bushpath.anamnesis.namenode;

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

    protected Map<String, HdfsProtos.DatanodeInfoProto> datanodes;
    protected Map<String, Long> mostRecentHeartbeatMillis;

    public DatanodeManager() {
        this.lock = new ReentrantReadWriteLock();
        this.random = new Random();
        this.datanodes = new HashMap<>();
        this.mostRecentHeartbeatMillis = new HashMap<>();
    }

    public void processRegistration(
            DatanodeProtocolProtos.RegisterDatanodeRequestProto req) {
        lock.writeLock().lock();
        try {
            HdfsProtos.DatanodeIDProto datanodeIDProto = 
                req.getRegistration().getDatanodeID();

            // retrieve datanode uuid
            String datanodeUuid = datanodeIDProto.getDatanodeUuid();
            logger.info("registering node '" + datanodeUuid + "'");

            if (datanodeUuid.isEmpty()) {
                // TODO - if not datanode uuid provided -> error
            }
 
            // store datanodesIDProto and blocks
            if (this.datanodes.containsKey(datanodeUuid)) {
                // TODO - datanode already registered -> error
            } else {
                HdfsProtos.DatanodeInfoProto datanodeInfoProto =
                    HdfsProtos.DatanodeInfoProto.newBuilder()
                        .setId(datanodeIDProto)
                        .setLastUpdate(System.currentTimeMillis())
                        .build();

                this.datanodes.put(datanodeUuid, datanodeInfoProto);
            }
        } finally {
            lock.writeLock().unlock();
        }
    }

    public void processHeartbeat(DatanodeProtocolProtos.HeartbeatRequestProto req) {
        lock.writeLock().lock();
        try {
            HdfsProtos.DatanodeIDProto datanodeIDProto =
                req.getRegistration().getDatanodeID();

            // retrieve datanode uuid
            String datanodeUuid = datanodeIDProto.getDatanodeUuid();

            // update last updated
            HdfsProtos.DatanodeInfoProto datanodeInfoProto =
                HdfsProtos.DatanodeInfoProto.newBuilder()
                    .mergeFrom(this.datanodes.get(datanodeUuid))
                    .setLastUpdate(System.currentTimeMillis())
                    .build();

            this.datanodes.put(datanodeUuid, datanodeInfoProto);
        } finally {
            lock.writeLock().unlock();
        }
    }

    public HdfsProtos.DatanodeInfoProto storeBlock(long blockId, long blockSize,
            List<String> favoredNodes) {

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
        return (HdfsProtos.DatanodeInfoProto) array[this.random.nextInt(array.length)];
    }
}
