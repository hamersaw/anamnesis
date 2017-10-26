package com.bushpath.anamnesis.datatransfer;

import com.google.common.primitives.Ints;
import com.google.common.primitives.Shorts;
import org.apache.hadoop.hdfs.protocol.proto.DataTransferProtos;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.OutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;

public class ChunkPacketHeader {
    // variables for packet length
    private static final int PKT_LENGTHS_LEN = Ints.BYTES + Shorts.BYTES;
    private static final int MAX_PROTO_SIZE =
        DataTransferProtos.PacketHeaderProto.newBuilder()
            .setOffsetInBlock(0)
            .setSeqno(0)
            .setLastPacketInBlock(false)
            .setDataLen(0)
            .setSyncBlock(false)
            .build()
            .getSerializedSize();
    public static final int PKT_MAX_HEADER_LEN = PKT_LENGTHS_LEN + MAX_PROTO_SIZE;

    private int packetLen;
    private DataTransferProtos.PacketHeaderProto packetHeaderProto;

    public ChunkPacketHeader(int packetLen,
            DataTransferProtos.PacketHeaderProto packetHeaderProto) {
        this.packetLen = packetLen;
        this.packetHeaderProto = packetHeaderProto;
    }

    public ChunkPacketHeader(int packetLen, long offsetInBlock, long seqno,
            boolean lastPacketInBlock, int dataLen, boolean syncBlock) {
        this.packetLen = packetLen;

        // create packet header protobuf in constructer
        DataTransferProtos.PacketHeaderProto.Builder builder =
            DataTransferProtos.PacketHeaderProto.newBuilder()
                .setOffsetInBlock(offsetInBlock)
                .setSeqno(seqno)
                .setLastPacketInBlock(lastPacketInBlock)
                .setDataLen(dataLen);

        if (syncBlock) {
            builder.setSyncBlock(true);
        }

        this.packetHeaderProto = builder.build();
    }

    public int getPacketLen() {
        return this.packetLen;
    }

    public long getOffsetInBlock() {
        return this.packetHeaderProto.getOffsetInBlock();
    }

    public long getSeqNo() {
        return this.packetHeaderProto.getSeqno();
    }

    public boolean isLastPacketInBlock() {
        return this.packetHeaderProto.getLastPacketInBlock();
    }

    public int getDataLen() {
        return this.packetHeaderProto.getDataLen();
    }

    public boolean getSyncBlock() {
        return this.packetHeaderProto.getSyncBlock();
    }

    public int getSerializedSize() {
        return PKT_LENGTHS_LEN + this.packetHeaderProto.getSerializedSize();
    }

    public void write(DataOutputStream out) throws IOException {
        out.writeInt(this.packetLen);
        out.writeShort((short) this.getSerializedSize());

        this.packetHeaderProto.writeTo(out);
    }

    public static ChunkPacketHeader read(DataInputStream in) throws IOException {
        // read lengths
        int packetLen = in.readInt();
        short packetHeaderLen = in.readShort();

        // read header into array and parse into protobuf
        byte[] headerArray = new byte[packetHeaderLen - PKT_LENGTHS_LEN];
        in.read(headerArray);
        DataTransferProtos.PacketHeaderProto packetHeaderProto =
            DataTransferProtos.PacketHeaderProto.parseFrom(headerArray);

        return new ChunkPacketHeader(packetLen, packetHeaderProto);
    }
}
