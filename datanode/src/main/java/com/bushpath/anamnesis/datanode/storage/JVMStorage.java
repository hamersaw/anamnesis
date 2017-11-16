package com.bushpath.anamnesis.datanode.storage;

import java.util.HashMap;
import java.util.Map;
import java.util.logging.Logger;

public class JVMStorage extends Storage {
    private static final Logger logger = Logger.getLogger(JVMStorage.class.getName());
    private Map<Long, byte[]> blocks;

    public JVMStorage() {
        this.blocks = new HashMap<>();
    }

    @Override
    public void storeBlock(long blockId, byte[] block) {
        logger.info("storing block:" + blockId + " with length " + block.length);
        this.blocks.put(blockId, block);
    }

    @Override
    public byte[] getBlock(long blockId) {
        return this.blocks.get(blockId);
    }

    @Override
    public long getBlockLength(long blockId) {
        if (!this.blocks.containsKey(blockId)) {
            return 0l;
        }

        return this.blocks.get(blockId).length;
    }
}
