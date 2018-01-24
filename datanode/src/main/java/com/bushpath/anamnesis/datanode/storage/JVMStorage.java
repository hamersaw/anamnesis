package com.bushpath.anamnesis.datanode.storage;

import org.apache.hadoop.hdfs.protocol.proto.DatanodeProtocolProtos;
import org.apache.hadoop.hdfs.protocol.proto.HdfsProtos;

import com.bushpath.anamnesis.datanode.inflator.Inflator;

import java.io.ByteArrayOutputStream;
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
        Block block = new Block(blockId, generationStamp, bytes);
        this.blocks.put(blockId, block);
    }

    @Override
    public void storeBlock(long blockId, long generationStamp, double[][] means,
            double[][] standardDeviations, long[] recordCounts, Inflator inflator)
            throws IOException {
        logger.info("storing block '" + blockId + "'");
        Block block = new Block(blockId, generationStamp, means,
            standardDeviations, recordCounts, inflator);

        // if justInTimeInflation is disabled inflate
        if (!this.justInTimeInflation) {
            block.inflate();
        }

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

    protected class Block {
        long blockId;
        long generationStamp;
        double[][] means;
        double[][] standardDeviations;
        long[] recordCounts;
        Inflator inflator;

        byte[] bytes;

        public Block(long blockId, long generationStamp, byte[] bytes) {
            this.blockId = blockId;
            this.generationStamp = generationStamp;
            this.bytes = bytes;
        }

        public Block(long blockId, long generationStamp, double[][] means, 
                double[][] standardDeviations, long[] recordCounts, Inflator inflator) {
            this.blockId = blockId;
            this.generationStamp = generationStamp;
            this.means = means;
            this.standardDeviations = standardDeviations;
            this.recordCounts = recordCounts;
            this.inflator = inflator;

            this.bytes = null;
        }

        public byte[] getBytes() throws IOException {
            // if not memory resident -> compute
            if (this.bytes == null) {
                this.inflate();
            }
 
            return this.bytes;
        }

        public long getLength() {
            // if memory resident -> return length
            if (this.bytes != null) {
                return this.bytes.length;
            }

            // otherwise compute length
            long length = 0;
            for (int i=0; i<this.recordCounts.length; i++) {
                length += this.inflator.getLength(this.means[i],
                    this.standardDeviations[i], this.recordCounts[i]);
            }

            return length;
        }

        private void inflate() throws IOException {
            // compute bytes
            ByteArrayOutputStream bytesOut = new ByteArrayOutputStream();

            for (int i=0; i<this.recordCounts.length; i++) {
                byte[] bytes = this.inflator.inflate(this.means[i],
                    this.standardDeviations[i], this.recordCounts[i]);

                bytesOut.write(bytes);
            }

            this.bytes = bytesOut.toByteArray();
            bytesOut.close();
        }
    }
}
