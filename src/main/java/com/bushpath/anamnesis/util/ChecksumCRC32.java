package com.bushpath.anamnesis.util;

import java.nio.ByteBuffer;
import java.util.zip.CRC32;

public class ChecksumCRC32 extends Checksum {
    public ChecksumCRC32() {
        super(4);
    }

    public byte[] compute(byte[] b, int off, int len) {
        CRC32 checksum = new CRC32();
        checksum.update(b, off, len);

        ByteBuffer buffer = ByteBuffer.allocate(Long.BYTES);
        buffer.putLong(checksum.getValue());
        return buffer.array();
    }
}
