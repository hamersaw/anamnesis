package com.bushpath.anamnesis.datanode.storage;

import org.apache.hadoop.hdfs.protocol.proto.DatanodeProtocolProtos;
import org.apache.hadoop.hdfs.protocol.proto.HdfsProtos;

import com.bushpath.anamnesis.datanode.inflator.Inflator;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Logger;

public class JVMStorage extends Storage {
    private static final Logger logger = Logger.getLogger(JVMStorage.class.getName());
    private Map<Long, Block> blocks;

    public JVMStorage(String storageUuid, boolean justInTimeInflation) {
        super(storageUuid, HdfsProtos.StorageTypeProto.RAM_DISK, justInTimeInflation);

        this.blocks = new HashMap<>();
    }

    @Override
    public void storeBlock(long blockId, long generationStamp, byte[] bytes) {
        logger.info("storing block '" + blockId + "' with length " + bytes.length);
        Block block = new RawBlock(blockId, generationStamp, bytes);
        this.blocks.put(blockId, block);
    }

    @Override
    public void storeBlock(long blockId, long generationStamp, double[][] means,
            double[][] standardDeviations, long[] recordCounts, Inflator inflator)
            throws IOException {
        logger.info("storing block '" + blockId + "'");
        Block block = new StatisticsBlock(blockId, generationStamp, means,
            standardDeviations, recordCounts, inflator, this.justInTimeInflation);

        this.blocks.put(blockId, block);
    }

    @Override
    public byte[] getBlock(long blockId) throws IOException {
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
