package com.bushpath.anamnesis.datanode.storage;

import java.io.IOException;

public abstract class Block {
    protected long blockId;
    protected long generationStamp;
    protected byte[] bytes;

    public Block(long blockId, long generationStamp, byte[] bytes) {
        this.blockId = blockId;
        this.generationStamp = generationStamp;
        this.bytes = bytes;
    }

    public abstract byte[] getBytes() throws IOException;
    public abstract long getLength();
}
