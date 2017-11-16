package com.bushpath.anamnesis.util;

import org.apache.hadoop.hdfs.protocol.proto.HdfsProtos;

public class ChecksumFactory {
    public static Checksum buildDefaultChecksum() {
        return buildChecksum(HdfsProtos.ChecksumTypeProto.CHECKSUM_CRC32);
    }

    public static Checksum buildChecksum(HdfsProtos.ChecksumTypeProto type) {
        switch (type) {
        case CHECKSUM_CRC32:
            return new ChecksumJavaCRC32C();
        default:
            System.out.println("CHECKSUM TYPE DOES NOT EXIST");
            return null;
        }
    }
}
