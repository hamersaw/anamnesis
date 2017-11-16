package com.bushpath.anamnesis.datanode.storage;

import org.apache.hadoop.hdfs.protocol.proto.DatanodeProtocolProtos;
import org.apache.hadoop.hdfs.protocol.proto.HdfsProtos;

public class TmpfsStorage extends Storage {
    public TmpfsStorage(String storageUuid) {
        super(storageUuid, HdfsProtos.StorageTypeProto.RAM_DISK);
    }

    @Override
    public void storeBlock(long blockId, byte[] block, long generationStamp) {
        // TODO
    }

    @Override
    public byte[] getBlock(long blockId) {
        // TODO
        return null;
    }

    @Override
    public long getBlockLength(long blockId) {
        // TODO
        return 0l;
    }

    @Override
    public DatanodeProtocolProtos.StorageBlockReportProto toStorageBlockReportProto() {
        // TODO - complete
        return null;
    }
}
