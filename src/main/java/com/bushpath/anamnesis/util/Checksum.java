package com.bushpath.anamnesis.util;

public abstract class Checksum {
    public abstract long compute(byte[] b, int off, int len);
}
