package com.bushpath.anamnesis.client;

import org.apache.hadoop.hdfs.protocol.proto.DataTransferProtos;

import com.bushpath.anamnesis.datatransfer.BlockOutputStream;
import com.bushpath.anamnesis.datatransfer.DataTransferProtocol;

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

    private byte[] buffer;
    private int index;

    public AnamnesisOutputStream(AnamnesisClient anamnesisClient, String path, 
            int blockSize, List<String> favoredNodes) throws IOException {
        this.anamnesisClient = anamnesisClient;
        this.path = path;
        this.blockSize = blockSize;
        this.favoredNodes = favoredNodes;

        this.buffer = new byte[blockSize];
        this.index = 0;
    }
    
    @Override
    public void write(int b) throws IOException {
        // write block if buffer is full
        if (this.index == this.buffer.length) {
            this.writeBlock();
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
        System.out.println("anamnesis write " + len + " bytes");
        int bytesWrote = 0;
        int bIndex = off;
        while (bytesWrote < len) {
            // write block if buffer is full
            if (this.index == this.buffer.length) {
                this.writeBlock();
            }

            // copy bytes from b to buffer
            int copyLen = Math.min(this.buffer.length - this.index, len - bytesWrote);
            System.arraycopy(b, bIndex, this.buffer, this.index, copyLen);
            bytesWrote += copyLen;
            bIndex += copyLen;
            this.index += copyLen;
        }
    }

    private void writeBlock() throws IOException {
        // get locations from namenode
        List<Location> locations = 
            this.anamnesisClient.addBlock(this.path, this.favoredNodes);

        // write block (stop on a successful write)
        for (Location location: locations) {
            System.out.println("writing block to " + location.getIpAddr() 
                + ":" + location.getPort());

            Socket socket = new Socket(location.getIpAddr(), location.getPort());
            DataInputStream in = new DataInputStream(socket.getInputStream());
            DataOutputStream out = new DataOutputStream(socket.getOutputStream());

            DataTransferProtocol.sendWriteOp(out, 
                DataTransferProtos.OpWriteBlockProto.BlockConstructionStage.PIPELINE_CLOSE,
                    "POOL_ID", -1l, -1l, "CLIENT");

            DataTransferProtos.BlockOpResponseProto response =
                DataTransferProtocol.recvBlockOpResponse(in);

            System.out.println("\tblock write status: " + response.getStatus());

            // send block chunks
            BlockOutputStream blockOut = new BlockOutputStream(out);
            blockOut.write(this.buffer, 0, this.index);
            blockOut.close();

            socket.close();

            // reset buffer index
            this.index = 0;
            break; // break on successful write
        }
    }

    @Override
    public void close() throws IOException {
        if (this.index != 0) {
            writeBlock();
        }
    }
}
