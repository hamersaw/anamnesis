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
        NameSystemFile file = nameSystem.getFile(path);

        // create block
        long blockId = random.nextLong(),
             generationStamp = System.currentTimeMillis();
        Block block = new Block(blockId, System.currentTimeMillis(), 
            file.blockSize * file.blocks.size());

        // find datanodes to store at
        HdfsProtos.DatanodeInfoProto datanodeInfoProto =
            this.datanodeManager.storeBlock(blockId, file.blockSize, favoredNodes);

        block.addLoc(datanodeInfoProto);

        file.blocks.add(blockId);
        this.blocks.put(blockId, block);
        return block;
    }
}
