package com.bushpath.anamnesis.datatransfer;

import com.bushpath.anamnesis.util.Checksum;

import java.io.DataOutputStream;
import java.io.IOException;
import java.io.OutputStream;

public class BlockOutputStream extends OutputStream {
    private DataOutputStream out;
    private Checksum checksum;
    private byte[] buffer;
    private int index;
    private long sequenceNumber, offsetInBlock;

    public BlockOutputStream(DataOutputStream out) {
        this.out = out;
        this.checksum = new Checksum(ChunkPacket.CHUNK_SIZE);
        this.buffer = new byte[ChunkPacket.CHUNK_SIZE * ChunkPacket.CHUNKS_PER_PACKET];
        this.index = 0;
        this.sequenceNumber = 0;
        this.offsetInBlock = 0;
    }

    @Override
    public void write(int b) throws IOException {
            // write chunks if buffer is full
        if (this.index == this.buffer.length) {
            this.writeChunks(false, false);
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
            // write chunks if buffer is full
            if (this.index == this.buffer.length) {
                this.writeChunks(false, false);
            }

            // copy bytes from b to buffer
            int copyLen = Math.min(this.buffer.length - this.index, len - bytesWrote);
            System.arraycopy(b, bIndex, this.buffer, this.index, copyLen);
            bytesWrote += copyLen;
            bIndex += copyLen;
            this.index += copyLen;
        }
    }

    private void writeChunks(boolean writePartial, boolean lastPacketInBlock)
            throws IOException {
        int writeLength = writePartial ? this.index : this.index % ChunkPacket.CHUNK_SIZE;
 
        // write chunk to chunk packet
        ChunkPacket packet = new ChunkPacket(this.sequenceNumber, this.offsetInBlock,
            lastPacketInBlock, this.checksum.getBytesPerChecksum());

        packet.writeData(this.buffer, 0, writeLength);
        // TODO - write checksums
        packet.write(this.out);

        // push bytes down buffer if necessary and reset index
        if (writeLength != this.buffer.length) {
            System.arraycopy(this.buffer, writeLength, this.buffer, 0, 
                this.index - writeLength);
        }

        this.index = 0;
    }

    @Override
    public void close() throws IOException {
        writeChunks(true, true);
    }
}
