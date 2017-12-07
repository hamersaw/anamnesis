package com.bushpath.anamnesis.checksum;

import java.util.zip.CRC32;

public class ChecksumCRC32 extends Checksum {
    @Override
    public long compute(byte[] b, int off, int len) {
        CRC32 checksum = new CRC32();
        checksum.reset();
        checksum.update(b, off, len);
        return checksum.getValue();
    }
}
