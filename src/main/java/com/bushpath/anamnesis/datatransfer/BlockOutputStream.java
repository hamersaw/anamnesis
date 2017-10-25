package com.bushpath.anamnesis.datatransfer;

import com.bushpath.anamnesis.util.Checksum;

import java.io.DataOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.Socket;

public class BlockOutputStream extends OutputStream {
    public static final int CHUNKS_IN_BUFFER = 9;
    private Socket socket;
    private int blockSize, chunkSize;
    private Checksum checksum;
    private byte[] buffer;
    private int index;
    private long sequenceNumber, offsetInBlock;

    public BlockOutputStream(Socket socket, int blockSize, int chunkSize) {
        this.socket = socket;
        this.blockSize = blockSize;
        this.chunkSize = chunkSize;
        this.checksum = new Checksum(chunkSize);
        this.buffer = new byte[chunkSize * CHUNKS_IN_BUFFER];
        this.index = 0;
        this.sequenceNumber = 0;
        this.offsetInBlock = 0;
    }

    @Override
    public void write(int b) throws IOException {
        if (this.index + 1 >= this.buffer.length) {
            this.writeChunks(false, false);
        }

        this.buffer[this.index] = (byte) b;
        this.index += 1;
    }

    @Override
    public void write(byte[] b, int off, int len) throws IOException {
        if (this.index + len >= this.buffer.length) {
            this.writeChunks(false, false);
        }
 
        System.arraycopy(this.buffer, this.index, b, off, len);
        this.index += len;
    }

    private void writeChunks(boolean writePartial, boolean lastPacketInBlock)
            throws IOException {
        int writeLength = writePartial ? this.index : this.index % this.chunkSize;
 
        // write chunk to chunk packet
        ChunkPacket packet = new ChunkPacket(this.sequenceNumber, this.offsetInBlock,
            CHUNKS_IN_BUFFER, -1, this.chunkSize, lastPacketInBlock);

        packet.writeData(this.buffer, 0, writeLength);
        // TODO - write checksums
        DataOutputStream out = new DataOutputStream(this.socket.getOutputStream());
        packet.write(out);

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
