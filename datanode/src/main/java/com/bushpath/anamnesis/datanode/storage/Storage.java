package com.bushpath.anamnesis.datanode.storage;

public abstract class Storage {
    public abstract void storeBlock(long blockId, byte[] block);
    public abstract byte[] getBlock(long blockId);
    public abstract long getBlockLength(long blockId);
}
