package com.bushpath.anamnesis.util;

import java.nio.ByteBuffer;
import java.util.zip.CRC32;

public class ChecksumCRC32 extends Checksum {
    public ChecksumCRC32() {
        super(4);
    }

    @Override
    public byte[] compute(byte[] b, int off, int len) {
        CRC32 checksum = new CRC32();
        checksum.reset();
        checksum.update(b, off, len);
        int calculated = (int) checksum.getValue();

        byte[] checksumBytes = new byte[4];
        checksumBytes[0] = (byte) (calculated >> 24);
        checksumBytes[1] = (byte) (calculated >> 16);
        checksumBytes[2] = (byte) (calculated >> 8);
        checksumBytes[3] = (byte) (calculated);

        // TODO - TMP
        //byte[] checksumBytes = hexStringToByteArray("9c8480d8");

        StringBuilder sb = new StringBuilder();
        for (byte checksumByte: checksumBytes) {
            sb.append(String.format("%02X ", checksumByte));
        }
        System.out.println("CHECKSUM: " + sb.toString() + " : " + calculated + ":" + off + ":" + len);

        return checksumBytes;

        /*ByteBuffer buffer = ByteBuffer.allocate(Long.BYTES);
        buffer.putLong(checksum.getValue());
        return buffer.array();*/
    }

    /*public byte[] hexStringToByteArray(String s) {
        int len = s.length();
        byte[] data = new byte[len / 2];
        for (int i = 0; i < len; i += 2) {
            data[i / 2] = (byte) ((Character.digit(s.charAt(i), 16) << 4)
                                 + Character.digit(s.charAt(i+1), 16));
        }
        return data;
    }*/
}
