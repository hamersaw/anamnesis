package com.bushpath.anamnesis.namenode;

import org.apache.hadoop.hdfs.protocol.proto.HdfsProtos;

import com.bushpath.anamnesis.namenode.Datanode;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class BlockManager {
    private Map<Long, Block> blocks;

    public BlockManager() {
        this.blocks = new HashMap<>();
    }

    public void add(Block block) {
        this.blocks.put(block.getBlockId(), block);
    }

    public void delete(long blockId) {
        this.blocks.remove(blockId);
    }

    public Block get(long blockId) {
        return this.blocks.get(blockId);
    }

    public boolean contains(long blockId) {
        return this.blocks.containsKey(blockId);
    }

    public List<Block> getDatanodeBlocks(String datanodeUuid) {
        List<Block> blocks = new ArrayList<>();
        for (Map.Entry<Long, Block> entry : this.blocks.entrySet()) {
            boolean datanodeFound = false;
            for (Datanode datanode : entry.getValue().getLocs()) {
                if (datanode.getDatanodeUuid().equals(datanodeUuid)) {
                    datanodeFound = true;
                    break;
                }
            }

            blocks.add(entry.getValue());
        }

        return blocks;
    }
}
