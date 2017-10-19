package com.bushpath.anamnesis.namenode;

import org.apache.hadoop.hdfs.protocol.proto.HdfsProtos;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

public class BlockManager {
    private DatanodeManager datanodeManager;
    private NameSystem nameSystem;
    private Random random;

    private Map<Long, Block> blocks;

    public BlockManager(DatanodeManager datanodeManager, NameSystem nameSystem) {
        this.datanodeManager = datanodeManager;
        this.nameSystem = nameSystem;
        this.random = new Random();

        this.blocks = new HashMap<>();
    }

    public Block createBlock(String path, List<String> favoredNodes) throws Exception {
        // retrieve file for block
        NSItem item = nameSystem.getFile(path);
        if (item.getType() != NSItem.Type.FILE) {
            throw new Exception("file '" + path + "' is not of type 'FILE'");
        }
        NSFile file = (NSFile) item;

        // create block
        long blockId = random.nextLong(),
             generationStamp = System.currentTimeMillis();
        Block block = new Block(blockId, System.currentTimeMillis(), 
            file.getBlockSize() * file.getBlockCount());

        // find datanodes to store at
        HdfsProtos.DatanodeInfoProto datanodeInfoProto =
            this.datanodeManager.storeBlock(blockId, file.getBlockSize(), favoredNodes);

        block.addLoc(datanodeInfoProto);

        file.addBlock(blockId);
        this.blocks.put(blockId, block);
        return block;
    }

    public Block getBlock(long blockId) {
        return this.blocks.get(blockId);
    }
}
