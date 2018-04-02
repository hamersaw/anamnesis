package com.bushpath.anamnesis.datanode.storage;

import org.apache.hadoop.hdfs.protocol.proto.DatanodeProtocolProtos;
import org.apache.hadoop.hdfs.protocol.proto.HdfsProtos;

import com.bushpath.anamnesis.datanode.inflator.Inflator;

import java.io.IOException;
import java.util.Collection;

public abstract class Storage {
    protected String storageUuid;
    protected HdfsProtos.StorageTypeProto storageType;
    protected boolean justInTimeInflation;

    public Storage(String storageUuid, HdfsProtos.StorageTypeProto storageType,
            boolean justInTimeInflation) {
        this.storageUuid = storageUuid;
        this.storageType = storageType;
        this.justInTimeInflation = justInTimeInflation;
    }

    public abstract void storeBlock(Block block) throws IOException;
    public abstract void deleteBlock(long blockId) throws IOException;
    public abstract byte[] getBlockBytes(long blockId) throws IOException;
    public abstract long getBlockLength(long blockId) throws IOException;
    public abstract Collection<Block> getBlocks();

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
