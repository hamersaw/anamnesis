package com.bushpath.anamnesis;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.BufferOverflowException;
import java.nio.channels.ClosedChannelException;

public class BlockChunkPacket {
    private long sequenceNumber;
    private long offsetInBlock;
    private boolean syncBlock;
    private int numChunks;
    private int maxChunks;
    private byte[] buf;
    private boolean lastPacketInBlock;

    private int checksumStart;
    private int checksumPos;
    private int dataStart;
    private int dataPos;

    public BlockChunkPacket(long sequenceNumber, long offsetInBlock, int chunksPerPacket,
            int checksumSize, byte[] buf, boolean lastPacketInBlock) {
        this.sequenceNumber = sequenceNumber;
        this.offsetInBlock = offsetInBlock;
        this.numChunks = 0;
        this.maxChunks = chunksPerPacket;
        this.buf = buf;
        this.lastPacketInBlock = lastPacketInBlock;

        this.checksumStart = BlockChunkPacketHeader.PKT_MAX_HEADER_LEN;
        this.checksumPos = this.checksumStart;
        this.dataStart = this.checksumStart + (chunksPerPacket * checksumSize);
        this.dataPos = this.dataStart;
        this.maxChunks = chunksPerPacket;
    }

    synchronized void writeData(byte[] inArray, int off, int len)
            throws ClosedChannelException {
        checkBuffer();
        if (this.dataPos + len > this.buf.length) {
            throw new BufferOverflowException();
        }

        System.arraycopy(inArray, off, this.buf, this.dataPos, len);
        this.dataPos += len;
    }

    synchronized void writeChecksum(byte[] inArray, int off, int len)
            throws ClosedChannelException {
        checkBuffer();
        if (len == 0) {
            return;
        }

        if (checksumPos + len > dataStart) {
            throw new BufferOverflowException();
        }

        System.arraycopy(inArray, off, this.buf, this.checksumPos, len);
        this.checksumPos += len;
    }

    synchronized void write(DataOutputStream out) throws IOException {
        checkBuffer();
        int dataLen = this.dataPos - this.dataStart;
        int checksumLen = this.checksumPos - this.checksumStart;
        int pktLen = 4 + dataLen + checksumLen; // 4 bytes in Integer

        // build header
        BlockChunkPacketHeader header = new BlockChunkPacketHeader(pktLen, 
            this.offsetInBlock, this.sequenceNumber, this.lastPacketInBlock,
            dataLen, this.syncBlock);

        if (checksumPos != dataStart) {
            // move checksum to flush with dataStart
            System.arraycopy(this.buf, this.checksumStart, this.buf,
                this.dataStart - checksumLen, checksumLen);

            this.checksumPos = dataStart;
            this.checksumStart = this.checksumPos - checksumLen;
        }

        // copy header into buffer immediately preceding the checksum
        int headerStart = checksumStart - header.getSerializedSize();
        System.arraycopy(header.getBytes(), 0, this.buf, headerStart,
            header.getSerializedSize());

        // write full packet to data stream
        out.write(this.buf, headerStart,
            header.getSerializedSize() + checksumLen + dataLen);
    }

    private synchronized void checkBuffer() throws ClosedChannelException {
        if (buf == null) {
            throw new ClosedChannelException();
        }
    }
}
