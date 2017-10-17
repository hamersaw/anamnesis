package com.bushpath.anamnesis.namenode;

import org.apache.hadoop.hdfs.protocol.proto.DatanodeProtocolProtos;
import org.apache.hadoop.hdfs.protocol.proto.HdfsProtos;

import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.logging.Logger;

public class DatanodeManager {
    private static final Logger logger =
        Logger.getLogger(DatanodeManager.class.getName());
    private static final int STORAGE_REPORT_BUFFER_SIZE = 100;

    protected ReadWriteLock lock;

    protected Map<String, HdfsProtos.DatanodeIDProto> datanodes;
    protected Map<String, List<HdfsProtos.StorageReportProto>> storageReports;
    protected Map<String, Long> mostRecentHeartbeatMillis;

    public DatanodeManager() {
        this.lock = new ReentrantReadWriteLock();
        this.datanodes = new HashMap<>();
        this.storageReports = new HashMap<>();
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
                this.datanodes.put(datanodeUuid, datanodeIDProto);
                this.storageReports.put(datanodeUuid, new ArrayList<>());
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

            // update most recent heartbeat millis
            this.mostRecentHeartbeatMillis.put(datanodeUuid, System.currentTimeMillis());

            if (!this.storageReports.containsKey(datanodeUuid)) {
                // TODO - datanode not registered -> error
            } else {
                List<HdfsProtos.StorageReportProto> list =
                    this.storageReports.get(datanodeUuid);

                List<HdfsProtos.StorageReportProto> reports = req.getReportsList();
                logger.info("adding " + reports.size() 
                        + " storage reports for datanode '" + datanodeUuid + "'");

                // TODO - ensure reports is smaller than STORAGE_REPORT_BUFFER_SIZE
                // remove old reports to keep list size under STORAGE_REPORT_BUFFER_SIZE
                int delta = (list.size() + reports.size())
                    - this.STORAGE_REPORT_BUFFER_SIZE;

                for (int i=delta; i>0; i--) {
                    list.remove(list.size() - 1);
                }

                // add new reports
                list.addAll(reports);
            }
        } finally {
            lock.writeLock().unlock();
        }
    }
}
