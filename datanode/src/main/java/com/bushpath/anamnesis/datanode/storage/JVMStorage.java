package com.bushpath.anamnesis.datanode.storage;

import org.apache.hadoop.hdfs.protocol.proto.DatanodeProtocolProtos;
import org.apache.hadoop.hdfs.protocol.proto.HdfsProtos;

import com.bushpath.anamnesis.datanode.inflator.Inflator;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class JVMStorage extends Storage {
    private Map<Long, Block> blocks;

    public JVMStorage(String storageUuid, boolean justInTimeInflation) {
        super(storageUuid, HdfsProtos.StorageTypeProto.RAM_DISK, justInTimeInflation);

        this.blocks = new HashMap<>();
    }

    @Override
    public void storeBlock(Block block) throws IOException {
        // if not just in time inflation -> inflate statistics block
        if (block instanceof StatisticsBlock && !this.justInTimeInflation) {
            ((StatisticsBlock) block).inflate();
        }

        this.blocks.put(block.getBlockId(), block);
    }

    @Override
    public void deleteBlock(long blockId) throws IOException {
        // don't throw error if block does not exist
        if (this.blocks.containsKey(blockId)) {
            this.blocks.remove(blockId);
        }
    }

    @Override
    public byte[] getBlockBytes(long blockId) throws IOException {
        if (!this.blocks.containsKey(blockId)) {
            throw new IOException("can not get block, block '"
                + blockId + "' does not exist");
        }

        return this.blocks.get(blockId).getBytes();
    }

    @Override
    public long getBlockLength(long blockId) throws IOException {
        if (!this.blocks.containsKey(blockId)) {
            throw new IOException("can not get block length, block '"
                + blockId + "' does not exist");
        }

        return this.blocks.get(blockId).getLength();
    }

    @Override
    public Collection<Block> getBlocks() {
        return this.blocks.values();
    }

    @Override
    public DatanodeProtocolProtos.StorageBlockReportProto toStorageBlockReportProto() {
        // compute block statistics
        List<Long> blockList = new ArrayList<>();
        for (Block block : blocks.values()) {
            blockList.add(block.blockId);
            blockList.add((long) block.getLength());
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
}
