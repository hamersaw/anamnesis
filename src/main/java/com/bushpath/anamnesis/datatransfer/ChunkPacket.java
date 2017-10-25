package com.bushpath.anamnesis.datatransfer;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.BufferOverflowException;
import java.nio.channels.ClosedChannelException;

public class ChunkPacket {
    private long sequenceNumber;
    private long offsetInBlock;
    private boolean syncBlock;
    private int numChunks;
    private int maxChunks;
    private boolean lastPacketInBlock;

    private byte[] buffer;
    private int checksumStart;
    private int checksumPos;
    private int dataStart;
    private int dataPos;

    public ChunkPacket(long sequenceNumber, long offsetInBlock, int chunksPerPacket,
            int checksumSize, int chunkSize, boolean lastPacketInBlock) {
        this(sequenceNumber, offsetInBlock, chunksPerPacket, checksumSize,
            new byte[ChunkPacketHeader.PKT_MAX_HEADER_LEN 
                + (chunksPerPacket * checksumSize) + (chunksPerPacket * chunkSize)],
            lastPacketInBlock);
    }

    public ChunkPacket(long sequenceNumber, long offsetInBlock, int chunksPerPacket,
            int checksumSize, byte[] buffer, boolean lastPacketInBlock) {
        this.sequenceNumber = sequenceNumber;
        this.offsetInBlock = offsetInBlock;
        this.numChunks = 0;
        this.maxChunks = chunksPerPacket;
        this.lastPacketInBlock = lastPacketInBlock;

        this.buffer = buffer;
        this.checksumStart = ChunkPacketHeader.PKT_MAX_HEADER_LEN;
        this.checksumPos = this.checksumStart;
        this.dataStart = this.checksumStart + (chunksPerPacket * checksumSize);
        this.dataPos = this.dataStart;
        this.maxChunks = chunksPerPacket;
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
        int headerStart = checksumStart - header.getSerializedSize();
        System.arraycopy(header.getBytes(), 0, this.buffer, headerStart,
            header.getSerializedSize());

        // write full packet to data stream
        out.write(this.buffer, headerStart,
            header.getSerializedSize() + checksumLen + dataLen);
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

    public static ChunkPacket read(DataInputStream in) throws IOException {
        // read header
        ChunkPacketHeader header = ChunkPacketHeader.read(in);
        byte[] buffer = new byte[header.getPacketLen() - header.getSerializedSize()];
        in.read(buffer);

        // TODO - validate checksums

        // create chunk packet
        return new ChunkPacket(header.getSeqNo(), header.getOffsetInBlock(), -1, -1, 
            buffer, header.isLastPacketInBlock());
    }

    private synchronized void checkBuffer() throws ClosedChannelException {
        if (this.buffer == null) {
            throw new ClosedChannelException();
        }
    }
}
