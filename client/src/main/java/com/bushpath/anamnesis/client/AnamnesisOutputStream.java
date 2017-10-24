package com.bushpath.anamnesis.client;

import org.apache.hadoop.hdfs.protocol.proto.DataTransferProtos;

import com.bushpath.anamnesis.DataTransferProtocol;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Logger;

public class AnamnesisOutputStream extends OutputStream {
    private static final Logger logger =
        Logger.getLogger(AnamnesisOutputStream.class.getName());

    private AnamnesisClient anamnesisClient;
    private String path;
    private int blockSize;
    private List<String> favoredNodes;

    private List<Byte> buffer;
    private Map<String, Socket> sockets;

    public AnamnesisOutputStream(AnamnesisClient anamnesisClient, String path, 
            int blockSize, List<String> favoredNodes) throws IOException {
        this.anamnesisClient = anamnesisClient;
        this.path = path;
        this.blockSize = blockSize;
        this.favoredNodes = favoredNodes;

        this.buffer = new ArrayList<>();
        this.sockets = new HashMap<>();
    }

    @Override
    public void write(byte[] b) throws IOException {
        // add bytes to buffer
        for (byte x: b) {
            this.buffer.add(x);
        }

        // write block if necessary
        while (this.checkWrite()) {
            this.writeBlock();
        }
    }

    @Override
    public void write(byte[] b, int off, int len) throws IOException {
        for (int i=off; i<off + len; i++) {
            this.buffer.add(b[i]);
        }
 
        // write block if necessary
        while (this.checkWrite()) {
            this.writeBlock();
        }
    }

    @Override
    public void write(int b) throws IOException {
        // add bytes to buffer
        ByteBuffer byteBuffer = ByteBuffer.allocate(4);
        byteBuffer.putInt(b);

        for (byte x: byteBuffer.array()) {
            this.buffer.add(x);
        }

        // write block if necessary
        while (this.checkWrite()) {
            this.writeBlock();
        }
    }

    private boolean checkWrite() {
        return this.buffer.size() >= 2 * this.blockSize;
    }

    private void writeBlock() throws IOException {
        List<Byte> block = this.buffer.subList(0,
            Math.min(this.blockSize, this.buffer.size()));

        // shift buffer
        if (block.size() != this.blockSize) {
           this.buffer.clear();
        } else {
            this.buffer = this.buffer.subList(block.size(), this.buffer.size());
        }

        // get locations from namenode
        List<Location> locations = 
            this.anamnesisClient.addBlock(this.path, this.favoredNodes);

        // write block (stop on a successful write)
        for (Location location: locations) {
            logger.info("writing block to " + location.getIpAddr() 
                + ":" + location.getPort());

            Socket socket = this.getSocket(location);
            DataTransferProtocol.sendWriteOp(
                new DataOutputStream(socket.getOutputStream()), "POOL_ID", -1l,
                -1l, "CLIENT");

            DataTransferProtos.BlockOpResponseProto response =
                DataTransferProtocol.recvBlockOpResponse(
                    new DataInputStream(socket.getInputStream()));

            logger.info("block write status: " + response.getStatus());

            // TODO - send block chunks

            break; // break on successful write
        }
    }

    private Socket getSocket(Location location) throws IOException {
        String socketAddr = location.getIpAddr() + ":" + location.getPort(); 
        if (!this.sockets.containsKey(socketAddr)) {
            Socket socket = new Socket(location.getIpAddr(), location.getPort());
            this.sockets.put(socketAddr, socket);
        }

        return this.sockets.get(socketAddr);
    }

    @Override
    public void close() throws IOException {
        // write last block if necessary
        while (this.buffer.size() != 0) {
            this.writeBlock();
        }

        this.anamnesisClient.close(path);
    }
}
