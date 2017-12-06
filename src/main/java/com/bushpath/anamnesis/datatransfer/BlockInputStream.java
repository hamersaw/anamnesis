package com.bushpath.anamnesis.datatransfer;

import org.apache.hadoop.hdfs.protocol.proto.DataTransferProtos;

import com.bushpath.anamnesis.checksum.Checksum;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.nio.BufferOverflowException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ArrayBlockingQueue;

public class BlockInputStream extends InputStream {
    private final static int CHUNK_PACKET_BUFFER_SIZE = 3;

    private DataInputStream in;
    private DataOutputStream out;
    private Checksum checksum;

    private byte[] buffer;
    private int startIndex, endIndex;
    private BlockingQueue<ChunkPacket> chunkPacketQueue;
    private BlockingQueue<Long> pipelineAckQueue;
    private Thread pipelineAckThread;
    private boolean lastPacketSeen;

    public BlockInputStream(DataInputStream in, DataOutputStream out,
            Checksum checksum) {
        this.in = in;
        this.out = out;
        this.checksum = checksum;

        this.startIndex = 0;
        this.endIndex = 0;
        this.chunkPacketQueue = new ArrayBlockingQueue<>(CHUNK_PACKET_BUFFER_SIZE);
        this.pipelineAckQueue = new ArrayBlockingQueue<>(CHUNK_PACKET_BUFFER_SIZE);
        this.lastPacketSeen = false;

        new ChunkReader().start();
        this.pipelineAckThread = new PipelineAckWriter();
        this.pipelineAckThread.start();
    }

    @Override
    public int read() throws IOException {
        // read packet if no bytes in buffer
        if (this.startIndex == this.endIndex) {
            if (this.readPacket() == 0) {
                throw new EOFException();
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
        if (this.chunkPacketQueue.isEmpty() && lastPacketSeen) {
            return 0;
        }

        // read next packet
        try {
            ChunkPacket packet = this.chunkPacketQueue.take();

            // refill buffer
            this.buffer = packet.getBuffer();
            this.startIndex = 0;
            this.endIndex = (int) packet.getBufferLength();

            // send ack
            while (!this.pipelineAckQueue.offer(packet.getSequenceNumber())) {};

            // check lastPacketInBlock
            this.lastPacketSeen = packet.getLastPacketInBlock();

            return this.endIndex;
        } catch(Exception e) {
            e.printStackTrace(); //TODO remove this
        }

        return 0;
    }

    @Override
    public void close() {
        try {
        this.pipelineAckThread.join();
        } catch(Exception e) {
            e.printStackTrace(); // TODO - remove
        }
    }

    private class ChunkReader extends Thread {
        @Override
        public void run() {
            boolean lastPacketSeen = false;

            // read packets from data input stream
            while (!lastPacketSeen) {
                try {
                    // read packet header
                    int packetLength = in.readInt();
                    short headerLength = in.readShort();
                    byte[] headerBuffer = new byte[headerLength];
                    in.readFully(headerBuffer);

                    DataTransferProtos.PacketHeaderProto packetHeaderProto =
                        DataTransferProtos.PacketHeaderProto.parseFrom(headerBuffer);

                    lastPacketSeen = packetHeaderProto.getLastPacketInBlock();

                    // read checksums
                    /*int checksumCount = (int) Math.ceil(packetHeaderProto.getDataLen()
                        / (double) DataTransferProtocol.CHUNK_SIZE);
                    List<Integer> checksums = new ArrayList<>();
                    for (int i=0; i<checksumCount; i++) {
                        checksums.add(in.readInt());
                    }*/
                    byte[] checksumBuffer =
                        new byte[packetLength - 4 - packetHeaderProto.getDataLen()];
                    in.readFully(checksumBuffer);

                    // read data
                    byte[] dataBuffer = new byte[packetHeaderProto.getDataLen()];
                    in.readFully(dataBuffer);

                    // TODO - fix checksum verification
                    /*// validate checksums
                    int checksumIndex = 0;
                    for (int i=0; i<checksumCount; i++) {
                        int checksumLength = Math.min(dataBuffer.length - checksumIndex,
                            DataTransferProtocol.CHUNK_SIZE);

                        int checksumValue = (int) checksum.compute(dataBuffer,
                            checksumIndex, checksumLength);

                        if (checksumValue != checksums.get(i)) {
                            throw new IOException("invalid chunk checksum. expecting "
                                   + checksumValue + " and got " + checksums.get(i));
                        }

                        checksumIndex += checksumLength;
                    }*/

                    ChunkPacket chunkPacket = new ChunkPacket(
                            packetHeaderProto.getLastPacketInBlock(),
                            packetHeaderProto.getSeqno(),
                            dataBuffer);

                    while(!chunkPacketQueue.offer(chunkPacket)) {}
                } catch(Exception e) {
                    e.printStackTrace();
                } 
            }
        }
    }

    private class PipelineAckWriter extends Thread {
        @Override
        public void run() {
            // loop until last packet has been seen
            Long sequenceNumber;
            while (!lastPacketSeen) {
                try {
                    // send pipeline ack
                    sequenceNumber = pipelineAckQueue.take();
                    DataTransferProtocol.sendPipelineAck(out, sequenceNumber);
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }
    }
}
