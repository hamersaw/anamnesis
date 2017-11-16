package com.bushpath.anamnesis.datanode.storage;

public class TmpfsStorage extends Storage {
    @Override
    public void storeBlock(long blockId, byte[] block) {
        // TODO
    }

    @Override
    public byte[] getBlock(long blockId) {
        // TODO
        return null;
    }

    @Override
    public long getBlockLength(long blockId) {
        // TODO
        return 0l;
    }
}
