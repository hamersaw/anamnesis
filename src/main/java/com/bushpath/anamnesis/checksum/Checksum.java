package com.bushpath.anamnesis.checksum;

public abstract class Checksum {
    public abstract long compute(byte[] b, int off, int len);
}
