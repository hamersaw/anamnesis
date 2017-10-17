package com.bushpath.anamnesis.protocol;

import org.apache.hadoop.hdfs.protocol.proto.HdfsServerProtos;

import java.util.List;

public class HdfsServer {
    public static HdfsServerProtos.StorageInfoProto buildStorageInfoProto(
            int layoutVersion, int namespceID, String clusterID, long cTime) {

        return HdfsServerProtos.StorageInfoProto.newBuilder()
            .setLayoutVersion(layoutVersion)
            .setNamespceID(namespceID)
            .setClusterID(clusterID)
            .setCTime(cTime)
            .build();
    }

    public static HdfsServerProtos.ExportedBlockKeysProto buildExportedBlockKeysProto(
            boolean isBlockTokenEnabled, long keyUpdateInterval, long tokenLifeTime,
            HdfsServerProtos.BlockKeyProto currentKey,
            List<HdfsServerProtos.BlockKeyProto> allKeys) {

        return HdfsServerProtos.ExportedBlockKeysProto.newBuilder()
            .setIsBlockTokenEnabled(isBlockTokenEnabled)
            .setKeyUpdateInterval(keyUpdateInterval)
            .setTokenLifeTime(tokenLifeTime)
            .setCurrentKey(currentKey)
            // TODO - all keys
            .build();
    }

    public static HdfsServerProtos.BlockKeyProto buildBlockKeyProto(int keyId,
            long expiryDate) {
        
        return HdfsServerProtos.BlockKeyProto.newBuilder()
            .setKeyId(keyId)
            .setExpiryDate(expiryDate)
            .build();
    }

    public static HdfsServerProtos.NNHAStatusHeartbeatProto
        buildNNHAStatusHeartbeatProto(
            HdfsServerProtos.NNHAStatusHeartbeatProto.State state, long txid) {

        return HdfsServerProtos.NNHAStatusHeartbeatProto.newBuilder()
            .setState(state)
            .setTxid(txid)
            .build();
    }
/*message NNHAStatusHeartbeatProto {
  enum State {
    ACTIVE = 0;
    STANDBY = 1;
  }
  required State state = 1;
  required uint64 txid = 2;
}*/
}