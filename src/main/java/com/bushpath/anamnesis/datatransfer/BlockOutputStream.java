package com.bushpath.anamnesis.datatransfer;

import org.apache.hadoop.hdfs.protocol.proto.DataTransferProtos;

import com.bushpath.anamnesis.util.Checksum;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.OutputStream;

public class BlockOutputStream extends OutputStream {
    private DataInputStream in;
    private DataOutputStream out;
    private Checksum checksum;
    private byte[] buffer;
    private int index;
    private long sequenceNumber, offsetInBlock;

    public BlockOutputStream(DataInputStream in, DataOutputStream out,
            Checksum checksum) {
        this.in = in;
        this.out = out;
        this.checksum = checksum;
        this.buffer = new byte[ChunkPacket.CHUNK_SIZE * ChunkPacket.CHUNKS_PER_PACKET];
        this.index = 0;
        this.sequenceNumber = 0;
        this.offsetInBlock = 0;
    }

    @Override
    public void write(int b) throws IOException {
        // write packet if buffer is full
        if (this.index == this.buffer.length) {
            this.writePacket(false);
        }

        this.buffer[this.index] = (byte) b;
        this.index += 1;
    }

    @Override
    public void write(byte[] b) throws IOException {
        this.write(b, 0, b.length);
    }

    @Override
    public void write(byte[] b, int off, int len) throws IOException {
        int bytesWrote = 0;
        int bIndex = off;

        while (bytesWrote < len) {
            // write packet if buffer is full
            if (this.index == this.buffer.length) {
                this.writePacket(false);
            }

            // copy bytes from b to buffer
            int copyLen = Math.min(this.buffer.length - this.index, len - bytesWrote);
            System.arraycopy(b, bIndex, this.buffer, this.index, copyLen);
            bytesWrote += copyLen;
            bIndex += copyLen;
            this.index += copyLen;
        }
    }

    @Override
    public void close() throws IOException {
        // if buffer is not empty write packet
        if (this.index != 0) {
            writePacket(false);
        }

        // write empty last packet
        writePacket(true);
        this.out.flush();
    }

    private void writePacket(boolean lastPacketInBlock) throws IOException {
        // compute packet length
        int checksumCount = 
            (int) Math.ceil(this.index / (double) ChunkPacket.CHUNK_SIZE);
        int packetLength = 4 + this.index + (checksumCount * 4);
        this.out.writeInt(packetLength);
        
        // write packet header
        DataTransferProtos.PacketHeaderProto packetHeaderProto =
            DataTransferProtos.PacketHeaderProto.newBuilder()
                .setOffsetInBlock(this.offsetInBlock)
                .setSeqno(this.sequenceNumber)
                .setLastPacketInBlock(lastPacketInBlock)
                .setDataLen(this.index)
                .setSyncBlock(false)
                .build();

        this.out.writeShort((short) packetHeaderProto.getSerializedSize());
        packetHeaderProto.writeTo(this.out);
 
        // write checksums
        System.out.println("WRITING CHECKSUMS");
        int checksumIndex = 0;
        while (checksumIndex < this.index - 1) {
            int checksumLength = Math.min(this.index - checksumIndex,
                ChunkPacket.CHUNK_SIZE);

            int checksum = (int) this.checksum.compute(this.buffer,
                checksumIndex, checksumLength);
            System.out.println("\tCHECKSUM "
                + (checksumIndex / ChunkPacket.CHUNK_SIZE) + ": " + checksum);
            this.out.writeInt(checksum);
            checksumIndex += checksumLength;
        }
 
        // write data
        this.out.write(this.buffer, 0, this.index);

        this.sequenceNumber += 1;
        this.offsetInBlock += this.index;
        this.index = 0;
    }
}
