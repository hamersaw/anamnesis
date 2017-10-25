package com.bushpath.anamnesis.datanode.storage;

public abstract class Storage {
    public abstract void storeBlock(long blockId, byte[] block);
}
