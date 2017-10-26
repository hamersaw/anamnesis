package com.bushpath.anamnesis.datatransfer;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.BufferOverflowException;
import java.nio.channels.ClosedChannelException;

public class ChunkPacket {
    public static final int CHUNKS_PER_PACKET = 9, CHUNK_SIZE = 1024;
    private long sequenceNumber;
    private long offsetInBlock;
    private boolean syncBlock;
    private boolean lastPacketInBlock;

    private byte[] buffer;
    private int checksumStart;
    private int checksumPos;
    private int dataStart;
    private int dataPos;

    public ChunkPacket(long sequenceNumber, long offsetInBlock, 
            boolean lastPacketInBlock, int checksumSize) {
        this.sequenceNumber = sequenceNumber;
        this.offsetInBlock = offsetInBlock;
        this.lastPacketInBlock = lastPacketInBlock;

        this.buffer = new byte[(CHUNKS_PER_PACKET * checksumSize) 
            + (CHUNKS_PER_PACKET * CHUNK_SIZE)];

        this.checksumStart = 0;
        this.checksumPos = this.checksumStart;
        this.dataStart = this.checksumStart + (CHUNKS_PER_PACKET * checksumSize);
        this.dataPos = this.dataStart;
    }

    public long getSequenceNumber() {
        return this.sequenceNumber;
    }

    public long getOffsetInBlock() {
        return this.offsetInBlock;
    }

    public boolean isLastPacketInBlock() {
        return this.lastPacketInBlock;
    }

    public void writeData(byte[] inArray, int off, int len)
            throws ClosedChannelException {
        checkBuffer();
        if (this.dataPos + len > this.buffer.length) {
            throw new BufferOverflowException();
        }

        System.arraycopy(inArray, off, this.buffer, this.dataPos, len);
        this.dataPos += len;
    }

    public void writeChecksum(byte[] inArray, int off, int len)
            throws ClosedChannelException {
        checkBuffer();
        if (len == 0) {
            return;
        }

        if (checksumPos + len > dataStart) {
            throw new BufferOverflowException();
        }

        System.arraycopy(inArray, off, this.buffer, this.checksumPos, len);
        this.checksumPos += len;
    }

    public void write(DataOutputStream out) throws IOException {
        checkBuffer();
        int dataLen = this.dataPos - this.dataStart;
        int checksumLen = this.checksumPos - this.checksumStart;
        int pktLen = 4 + dataLen + checksumLen; // 4 bytes in Integer

        // build header
        ChunkPacketHeader header = new ChunkPacketHeader(pktLen, 
            this.offsetInBlock, this.sequenceNumber, this.lastPacketInBlock,
            dataLen, this.syncBlock);

        if (checksumPos != dataStart) {
            // move checksum to flush with dataStart
            System.arraycopy(this.buffer, this.checksumStart, this.buffer,
                this.dataStart - checksumLen, checksumLen);

            this.checksumPos = dataStart;
            this.checksumStart = this.checksumPos - checksumLen;
        }

        // copy header into buffer immediately preceding the checksum
        header.write(out);
        out.write(this.buffer, checksumStart, checksumLen + dataLen);
    }

    public static ChunkPacket read(DataInputStream in) throws IOException {
        // read header
        ChunkPacketHeader header = ChunkPacketHeader.read(in);

        // create packet
        ChunkPacket packet = new ChunkPacket(header.getSeqNo(), 
            header.getOffsetInBlock(), header.isLastPacketInBlock(),  0);

        byte[] buffer = new byte[header.getPacketLen() - 4];
        in.readFully(buffer);

        packet.writeData(buffer, 0, buffer.length);

        // TODO - validate and write checksums

        return packet;
    }

    public int copyData(byte[] buffer, int off, int len) throws IOException {
        checkBuffer();
        if (this.dataPos - this.dataStart > len) {
            throw new BufferOverflowException();
        }

        // copy data from this buffer to the other
        int bytesRead = Math.min(len, this.dataPos - this.dataStart);
        System.arraycopy(this.buffer, this.dataStart, buffer, off, bytesRead);
        return bytesRead;
    }

    private synchronized void checkBuffer() throws ClosedChannelException {
        if (this.buffer == null) {
            throw new ClosedChannelException();
        }
    }
}
