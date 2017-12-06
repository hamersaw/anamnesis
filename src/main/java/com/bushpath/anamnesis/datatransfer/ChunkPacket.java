package com.bushpath.anamnesis.datatransfer;

public class ChunkPacket {
    private boolean lastPacketInBlock;
    private long sequenceNumber;
    private byte[] buffer;
    private long bufferLength;

    public ChunkPacket(boolean lastPacketInBlock, long sequenceNumber,
            byte[] buffer, long bufferLength) {
        this.lastPacketInBlock = lastPacketInBlock;
        this.sequenceNumber = sequenceNumber;
        this.buffer = buffer;
        this.bufferLength = bufferLength;
    }

    public boolean getLastPacketInBlock() {
        return this.lastPacketInBlock;
    }

    public long getSequenceNumber() {
        return this.sequenceNumber;
    }

    public byte[] getBuffer() {
        return this.buffer;
    }

    public long getBufferLength() {
        return this.buffer.length;
    }
}
