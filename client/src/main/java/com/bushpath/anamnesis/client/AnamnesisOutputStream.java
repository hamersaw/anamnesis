package com.bushpath.anamnesis.client;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

public class AnamnesisOutputStream extends OutputStream {
    private AnamnesisClient anamnesisClient;
    private String path;
    private int blockSize;
    private List<String> favoredNodes;

    private List<Byte> buffer;

    public AnamnesisOutputStream(AnamnesisClient anamnesisClient, String path, 
            int blockSize, List<String> favoredNodes) throws IOException {
        this.anamnesisClient = anamnesisClient;
        this.path = path;
        this.blockSize = blockSize;
        this.favoredNodes = favoredNodes;

        this.buffer = new ArrayList<>();
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

        // write block to locations
        System.out.println("TODO - write block");
        for (Location location: locations) {
            System.out.println("\t" + location.getIpAddr() + ":" + location.getPort());
        }
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
