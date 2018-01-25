package com.bushpath.anamnesis.datanode.storage;

import java.io.IOException;

import java.util.ArrayList;
import java.util.List;

public abstract class Block {
    protected long blockId;
    protected long generationStamp;
    protected byte[] bytes;
    protected List<Long> accessTimes;

    public Block(long blockId, long generationStamp, byte[] bytes) {
        this.blockId = blockId;
        this.generationStamp = generationStamp;
        this.bytes = bytes;
        this.accessTimes = new ArrayList<Long>();
    }

    public long getBlockId() {
        return this.blockId;
    }

    protected void addAccess() {
        this.accessTimes.add(System.currentTimeMillis());
    }

    public List<Long> getAccessTimes() {
        return this.accessTimes;
    }

    public abstract byte[] getBytes() throws IOException;
    public abstract long getLength();
}
