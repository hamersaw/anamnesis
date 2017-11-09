package com.bushpath.anamnesis.util;

public class Checksum {
    private int bytesInChecksum;

    public Checksum(int bytesInChecksum) {
        this.bytesInChecksum = bytesInChecksum;
    }

    public static int getBytesPerChecksum() {
        return 4;
    }
}
