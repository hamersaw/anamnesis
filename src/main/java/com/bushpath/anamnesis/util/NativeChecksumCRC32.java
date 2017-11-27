package com.bushpath.anamnesis.util;

public class NativeChecksumCRC32 extends Checksum {
    static {
        System.loadLibrary("crc");
    }

    @Override
    public long compute(byte[] b, int off, int len) {
        return (long) nativeCompute(b, off, len);
    }

    public native int nativeCompute(byte[] buffer, int offset, int length);
}
