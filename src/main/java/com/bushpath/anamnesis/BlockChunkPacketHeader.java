package com.bushpath.anamnesis;

import com.google.common.primitives.Ints;
import com.google.common.primitives.Shorts;
import org.apache.hadoop.hdfs.protocol.proto.DataTransferProtos;

import java.io.DataOutputStream;
import java.io.OutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;

public class BlockChunkPacketHeader {
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

    public BlockChunkPacketHeader(int packetLen, long offsetInBlock, long seqno,
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

    public byte[] getBytes() {
        // write packet to byte buffer
        ByteBuffer buf = ByteBuffer.allocate(this.getSerializedSize());
        try {
            buf.putInt(packetLen);
            buf.putShort((short) this.getSerializedSize());
            this.packetHeaderProto.writeTo(new ByteBufferOutputStream(buf));
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        return buf.array();
    }

    public void write(DataOutputStream out) throws IOException {
        out.writeInt(this.packetLen);
        out.writeShort(this.packetHeaderProto.getSerializedSize());
        this.packetHeaderProto.writeTo(out);
    }

    private class ByteBufferOutputStream extends OutputStream {
        private final ByteBuffer buf;

        public ByteBufferOutputStream(ByteBuffer buf) {
            this.buf = buf;
        }

        @Override
        public void write(int b) throws IOException {
            buf.put((byte) b);
        }

        @Override
        public void write(byte[] b, int off, int len) throws IOException {
            buf.put(b, off, len);
        }
    }
}
