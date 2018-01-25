package com.bushpath.anamnesis.datanode.storage;

import java.io.IOException;

public class RawBlock extends Block {
    public RawBlock(long blockId, long generationStamp, byte[] bytes) {
        super(blockId, generationStamp, bytes);
    }

    @Override
    public byte[] getBytes() throws IOException {
        this.addAccess();
        return this.bytes;
    }

    @Override
    public long getLength() {
        return this.bytes.length;
    }
}
