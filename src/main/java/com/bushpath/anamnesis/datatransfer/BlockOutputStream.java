package com.bushpath.anamnesis.datatransfer;

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
        // write chunks if buffer is full
        if (this.index == this.buffer.length) {
            this.writeChunks(false);
        }

        this.buffer[this.index] = (byte) b;
        this.index += 1;
    }

    @Override
    public void write(byte[] b) throws IOException {
        // write chunks if buffer is full
        if (this.index == this.buffer.length) {
            this.writeChunks(false);
        }

        this.write(b, 0, b.length);
        this.index += 1;
    }

    @Override
    public void write(byte[] b, int off, int len) throws IOException {
        int bytesWrote = 0;
        int bIndex = off;

        while (bytesWrote < len) {
            // write chunks if buffer is full
            if (this.index == this.buffer.length) {
                this.writeChunks(false);
            }

            // copy bytes from b to buffer
            int copyLen = Math.min(this.buffer.length - this.index, len - bytesWrote);
            System.arraycopy(b, bIndex, this.buffer, this.index, copyLen);
            bytesWrote += copyLen;
            bIndex += copyLen;
            this.index += copyLen;
        }
    }

    private void writeChunks(boolean lastPacketInBlock) throws IOException {
        System.out.println("WRITING CHUNK PACKET");
        // creeate chunk packet
        ChunkPacket packet = new ChunkPacket(this.sequenceNumber, this.offsetInBlock,
            lastPacketInBlock, this.checksum.getBytesPerChecksum());

        // write data to packet
        int writeIndex = 0;
        while (writeIndex < this.index - 1) {
            // get length of next chunk
            int writeLength = Math.min(this.index - writeIndex - 1,
                ChunkPacket.CHUNK_SIZE);

            // write chunk to packet with checksum
            packet.writeData(this.buffer, writeIndex, writeLength);
            byte[] checksumBytes =
                this.checksum.compute(this.buffer, writeIndex, writeLength);
            packet.writeChecksum(checksumBytes, 0, checksumBytes.length);
            System.out.println("\twrote chunk of size " + writeLength);

            writeIndex += writeLength;
        }

        System.out.println("writing chunk packet (" + this.sequenceNumber + ") with "
            + writeIndex + " bytes");
        System.out.println("\tlast packet in block: " + lastPacketInBlock);

        // write packet
        packet.write(this.out);

        this.sequenceNumber += 1;
        this.offsetInBlock += this.index;
        this.index = 0;
    }

    @Override
    public void close() throws IOException {
        // if buffer is not empty write packet
        if (this.index != 0) {
            writeChunks(false);
        }

        // write empty last packet
        writeChunks(true);
        this.out.flush();
    }
}
