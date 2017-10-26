package com.bushpath.anamnesis.datanode.storage;

import java.util.HashMap;
import java.util.Map;

public class JVMStorage extends Storage {
    private Map<Long, byte[]> blocks;

    public JVMStorage() {
        this.blocks = new HashMap<>();
    }

    @Override
    public void storeBlock(long blockId, byte[] block) {
        System.out.println("storing block:" + blockId + " with length " + block.length);
        this.blocks.put(blockId, block);
    }

    @Override
    public byte[] getBlock(long blockId) {
        return this.blocks.get(blockId);
    }
}
