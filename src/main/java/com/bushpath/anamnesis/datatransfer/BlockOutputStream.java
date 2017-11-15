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
        System.out.println("WRITING CHUNK");
        System.out.println("\tLAST_PACKET:" + lastPacketInBlock);
 
        // write chunk to chunk packet
        ChunkPacket packet = new ChunkPacket(this.sequenceNumber, this.offsetInBlock,
            lastPacketInBlock, this.checksum.getBytesPerChecksum());

        packet.writeData(this.buffer, 0, writeLength);
        if (this.index != 0) { // write checksum if there was any data written
            byte[] checksumBytes = this.checksum.compute(this.buffer, 0, writeLength);
            packet.writeChecksum(checksumBytes,
                this.checksum.getBytesPerChecksum(), 4); // TODO - fix hardcoding of 4
        }
        packet.write(this.out);
        
        // read ack (don't need!)
        //DataTransferProtocol.recvPipelineAck(this.in);

        // push bytes down buffer if necessary and reset index
        if (writeLength != this.buffer.length) {
            System.arraycopy(this.buffer, writeLength, this.buffer, 0, 
                this.index - writeLength);
        }

        this.index = 0;
    }

    @Override
    public void close() throws IOException {
        System.out.println("\t\tCLOSE:0");
        if (this.index != 0) {
            System.out.println("\t\tCLOSE:1");
            writeChunks(true, false);
        }

        System.out.println("\t\tCLOSE:2");
        writeChunks(true, true);
        this.out.flush();
    }
}
