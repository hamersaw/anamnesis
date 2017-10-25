package com.bushpath.anamnesis.datatransfer;

import com.bushpath.anamnesis.datatransfer.ChunkPacketHeader;

import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.Socket;
import java.nio.BufferOverflowException;

public class BlockInputStream extends InputStream {
    private Socket socket;
    private byte[] buffer;
    private int startIndex, endIndex;

    public BlockInputStream(Socket socket, int chunkSize) {
        this.socket = socket;
        this.startIndex = 0;
        this.endIndex = 0;
        this.buffer = new byte[BlockOutputStream.CHUNKS_IN_BUFFER * chunkSize];
    }

    @Override
    public int read() throws IOException {
        // if no data in buffer read next block
        if (this.startIndex + 1 >= this.endIndex) {
            this.readChunks();
        }

        // return 
        int value = (int) this.buffer[this.startIndex];
        this.startIndex++;
        return value;
    }

    public int read(byte[] b, int off, int len) throws IOException {
        int bytesRead = 0;
        while (bytesRead < len) {
            // if no data in current buffer read next block
            if (this.endIndex - this.startIndex <= 0) {
                this.readChunks();
            }

            // write data to buffer
            int pseudoEndIndex = Math.min(this.endIndex,
                this.startIndex + (len - bytesRead));
            System.arraycopy(this.buffer, this.startIndex, b, off + bytesRead,
                pseudoEndIndex - this.startIndex);
            bytesRead += pseudoEndIndex - this.startIndex;
        }

        return bytesRead;
    }

    private void readChunks() throws IOException {
        if (this.startIndex != this.endIndex) {
            // should not get here unless buffer is empty
            throw new BufferOverflowException();
        }
        DataInputStream in = new DataInputStream(this.socket.getInputStream());
        ChunkPacket packet = ChunkPacket.read(in);

        // copy data from most recent block
        this.endIndex = packet.copyData(this.buffer, 0, this.buffer.length);
        this.startIndex = 0;
    }
}
