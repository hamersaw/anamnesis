package com.bushpath.anamnesis.datatransfer;

public class ChunkPacket {
    private boolean lastPacketInBlock;
    private long sequenceNumber;
    private byte[] buffer;

    public ChunkPacket(boolean lastPacketInBlock, long sequenceNumber, byte[] buffer) {
        this.lastPacketInBlock = lastPacketInBlock;
        this.sequenceNumber = sequenceNumber;
        this.buffer = buffer;
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
