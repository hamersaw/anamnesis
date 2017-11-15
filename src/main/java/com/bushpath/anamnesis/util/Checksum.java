package com.bushpath.anamnesis.util;

public abstract class Checksum {
    private int bytesInChecksum;

    public Checksum(int bytesInChecksum) {
        this.bytesInChecksum = bytesInChecksum;
    }

    public abstract byte[] compute(byte[] b, int off, int len);

    public int getBytesPerChecksum() {
        return this.bytesInChecksum;
    }
}
