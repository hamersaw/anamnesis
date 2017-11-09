package com.bushpath.anamnesis.datatransfer;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.BufferOverflowException;

public class BlockInputStream extends InputStream {
    private DataInputStream in;
    private DataOutputStream out;
    private byte[] buffer;
    private int startIndex, endIndex;
    private boolean lastPacketSeen;

    public BlockInputStream(DataInputStream in, DataOutputStream out) {
        this.in = in;
        this.out = out;
        this.buffer = new byte[ChunkPacket.CHUNKS_PER_PACKET * ChunkPacket.CHUNK_SIZE];
        this.startIndex = 0;
        this.endIndex = 0;
        this.lastPacketSeen = false;
    }

    @Override
    public int read() throws IOException {
        // if no data in buffer read next block
        if (this.startIndex + 1 >= this.endIndex) {
            if (this.lastPacketSeen) {
                return 0;
            }

            this.readChunks();
        }

        // return requested byte
        int value = (int) this.buffer[this.startIndex];
        this.startIndex++;
        return value;
    }

    public int read(byte[] b) throws IOException {
        return this.read(b, 0, b.length);
    }

    @Override
    public int read(byte[] b, int off, int len) throws IOException {
        int bytesRead = 0;
        while (bytesRead < len) {
            // if no data in buffer read next block
            if (this.endIndex - this.startIndex <= 0) {
                if (this.lastPacketSeen) {
                    break;
                }

                if (this.readChunks() == 0) {
                    break;
                }
            }

            // write data to buffer
            int copyLen = Math.min(this.endIndex - this.startIndex, len - bytesRead);
            System.arraycopy(this.buffer, this.startIndex, b, off + bytesRead, copyLen);
            bytesRead += copyLen;
            this.startIndex += copyLen;
        }

        return bytesRead;
    }

    private int readChunks() throws IOException {
        if (this.startIndex != this.endIndex) {
            // should not get here unless buffer is empty
            throw new BufferOverflowException();
        }

        // read packet and send ack
        ChunkPacket packet = ChunkPacket.read(this.in);
        DataTransferProtocol.sendPipelineAck(this.out, packet.getSequenceNumber());
        if (packet.isLastPacketInBlock()) {
            this.lastPacketSeen = true;
        }

        // copy data from most recent block
        this.endIndex = packet.copyData(this.buffer, 0, this.buffer.length);
        this.startIndex = 0;
        return this.endIndex;
    }
}
