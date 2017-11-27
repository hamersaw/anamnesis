package com.bushpath.anamnesis.datatransfer;

import org.apache.hadoop.hdfs.protocol.proto.DataTransferProtos;

import com.bushpath.anamnesis.checksum.Checksum;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.BufferOverflowException;
import java.util.ArrayList;
import java.util.List;

public class BlockInputStream extends InputStream {
    private DataInputStream in;
    private DataOutputStream out;
    private Checksum checksum;
    private byte[] buffer;
    private int startIndex, endIndex;
    private boolean lastPacketSeen;

    public BlockInputStream(DataInputStream in, DataOutputStream out,
            Checksum checksum) {
        this.in = in;
        this.out = out;
        this.checksum = checksum;
        this.buffer = new byte[DataTransferProtocol.CHUNKS_PER_PACKET 
            * DataTransferProtocol.CHUNK_SIZE];
        this.startIndex = 0;
        this.endIndex = 0;
        this.lastPacketSeen = false;
    }

    @Override
    public int read() throws IOException {
        // read packet if no bytes in buffer
        if (this.startIndex == this.endIndex) {
            if (this.readPacket() == 0) {
                // TODO - throw EOF exception
            }
        }

        int value = this.buffer[this.startIndex];
        this.startIndex += 1;
        return value;
    }

    @Override
    public int read(byte[] b) throws IOException {
        return this.read(b, 0, b.length);
    }

    @Override
    public int read(byte[] b, int off, int len) throws IOException {
        int bytesRead = 0;
        int bIndex = 0;

        while(bytesRead < len) {
            // read packet if no bytes in buffer
            if (this.startIndex == this.endIndex) {
                if (this.readPacket() == 0) {
                    break;
                }
            }

            // copy bytes from this.buffer to b
            int copyLen = Math.min(this.endIndex - this.startIndex, len - bytesRead);
            System.arraycopy(this.buffer, this.startIndex, b, bIndex, copyLen);
            bytesRead += copyLen;
            bIndex += copyLen;
            this.startIndex += copyLen;
        }

        return bytesRead;
    }

    private int readPacket() throws IOException {
        if (this.lastPacketSeen) {
            return 0;
        }

        // read packet header
        int packetLength = this.in.readInt();
        short headerLength = this.in.readShort();
        byte[] headerBuffer = new byte[headerLength];
        this.in.readFully(headerBuffer);

        DataTransferProtos.PacketHeaderProto packetHeaderProto =
            DataTransferProtos.PacketHeaderProto.parseFrom(headerBuffer);

        // check lastPacketInBlock
        this.lastPacketSeen = packetHeaderProto.getLastPacketInBlock();

        // read checksums
        int checksumCount = (int) Math.ceil(packetHeaderProto.getDataLen()
            / (double) DataTransferProtocol.CHUNK_SIZE);
        List<Integer> checksums = new ArrayList<>();
        for (int i=0; i<checksumCount; i++) {
            checksums.add(this.in.readInt());
        }

        // read data
        in.readFully(this.buffer, 0, packetHeaderProto.getDataLen());
        this.startIndex = 0;
        this.endIndex = packetHeaderProto.getDataLen();

        // validate checksums
        int checksumIndex = 0;
        for (int i=0; i<checksumCount; i++) {
            int checksumLength = Math.min(this.endIndex - checksumIndex,
                DataTransferProtocol.CHUNK_SIZE);

            int checksum = (int) this.checksum.compute(this.buffer,
                checksumIndex, checksumLength);

            if (checksum != checksums.get(i)) {
                // TODO throw exception
                System.out.println("invalid checksum");
            }

            checksumIndex += checksumLength;
        }

        // send pipeline ack
        DataTransferProtocol.sendPipelineAck(this.out,
            packetHeaderProto.getSeqno());

        return this.endIndex;
    }
}
