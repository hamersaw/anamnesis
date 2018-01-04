package com.bushpath.anamnesis.datanode.storage;

import org.apache.hadoop.hdfs.protocol.proto.DatanodeProtocolProtos;
import org.apache.hadoop.hdfs.protocol.proto.HdfsProtos;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Logger;

public class JVMStorage extends Storage {
    private static final Logger logger = Logger.getLogger(JVMStorage.class.getName());
    private Map<Long, Block> blocks;

    public JVMStorage(String storageUuid) {
        super(storageUuid, HdfsProtos.StorageTypeProto.RAM_DISK);

        this.blocks = new HashMap<>();
    }

    @Override
    public void storeBlock(long blockId, byte[] block, long generationStamp) {
        logger.info("storing block:" + blockId + " with length " + block.length);
        this.blocks.put(blockId, new Block(blockId, block, generationStamp));
    }

    @Override
    public byte[] getBlock(long blockId) {
        return this.blocks.get(blockId).bytes;
    }

    @Override
    public long getBlockLength(long blockId) {
        if (!this.blocks.containsKey(blockId)) {
            return 0l;
        }

        return this.blocks.get(blockId).bytes.length;
    }

    @Override
    public DatanodeProtocolProtos.StorageBlockReportProto toStorageBlockReportProto() {
        // compute block statistics
        List<Long> blockList = new ArrayList<>();
        for (Block block : blocks.values()) {
            blockList.add(block.blockId);
            blockList.add((long) block.bytes.length);
            blockList.add(block.generationStamp);
            blockList.add(-1l); // TODO - replica state (if under construction)
        }

        // return proto
        return DatanodeProtocolProtos.StorageBlockReportProto.newBuilder()
            .setStorage(this.toDatanodeStorageProto())
            .addAllBlocks(blockList)
            .setNumberOfBlocks(this.blocks.size())
            .build();
    }

    @Override
    public HdfsProtos.StorageReportProto toStorageReportProto() {
        Runtime runtime = Runtime.getRuntime();

        return HdfsProtos.StorageReportProto.newBuilder()
            .setStorageUuid(this.storageUuid)
            .setCapacity(runtime.maxMemory())
            .setRemaining(runtime.freeMemory())
            .setStorage(this.toDatanodeStorageProto())
            .build();
    }

    private class Block {
        long blockId;
        byte[] bytes;
        long generationStamp;

        public Block(long blockId, byte[] bytes, long generationStamp) {
            this.blockId = blockId;
            this.bytes = bytes;
            this.generationStamp = generationStamp;
        }
    }
}
