package com.bushpath.anamnesis.namenode;

import org.apache.hadoop.hdfs.protocol.proto.HdfsProtos;

import com.bushpath.anamnesis.namenode.namesystem.NameSystem;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

public class BlockManager {
    private DatanodeManager datanodeManager;
    private NameSystem nameSystem;
    private Random random;

    private Map<Long, Block> blocks;

    public BlockManager(DatanodeManager datanodeManager,
            NameSystem nameSystem) {
        this.datanodeManager = datanodeManager;
        this.nameSystem = nameSystem;
        this.random = new Random();

        this.blocks = new HashMap<>();
    }

    public void add(Block block) {
        this.blocks.put(block.getBlockId(), block);
    }

    public Block get(long blockId) {
        return this.blocks.get(blockId);
    }
}
