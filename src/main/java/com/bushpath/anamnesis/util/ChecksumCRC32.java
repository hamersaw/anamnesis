package com.bushpath.anamnesis.util;

import java.util.zip.CRC32;

public class ChecksumCRC32 extends Checksum {
    @Override
    public long compute(byte[] b, int off, int len) {
        CRC32 checksum = new CRC32();
        checksum.reset();
        checksum.update(b, off, len);
        return checksum.getValue();

        /*int calculated = (int) checksum.getValue();
        byte[] checksumBytes = new byte[4];
        checksumBytes[0] = (byte) (calculated >> 24);
        checksumBytes[1] = (byte) (calculated >> 16);
        checksumBytes[2] = (byte) (calculated >> 8);
        checksumBytes[3] = (byte) (calculated);

        ByteBuffer buffer = ByteBuffer.allocate(Long.BYTES);
        buffer.putLong(checksum.getValue());
        return buffer.array();*/
    }
}
