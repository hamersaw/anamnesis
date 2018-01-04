package com.bushpath.anamnesis.datanode.storage;

import org.apache.hadoop.hdfs.protocol.proto.DatanodeProtocolProtos;
import org.apache.hadoop.hdfs.protocol.proto.HdfsProtos;

public abstract class Storage {
    protected String storageUuid;
    protected HdfsProtos.StorageTypeProto storageType;

    public Storage(String storageUuid, HdfsProtos.StorageTypeProto storageType) {
        this.storageUuid = storageUuid;
        this.storageType = storageType;
    }

    public abstract void storeBlock(long blockId, byte[] block, long generationStamp);
    public abstract byte[] getBlock(long blockId);
    public abstract long getBlockLength(long blockId);

    public HdfsProtos.DatanodeStorageProto toDatanodeStorageProto() {
        return HdfsProtos.DatanodeStorageProto.newBuilder()
            .setStorageUuid(this.storageUuid)
            .setStorageType(this.storageType)
            .build();
    }

    public abstract DatanodeProtocolProtos.StorageBlockReportProto 
        toStorageBlockReportProto();
    public abstract HdfsProtos.StorageReportProto toStorageReportProto();
}
