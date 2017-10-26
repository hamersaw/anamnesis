package com.bushpath.anamnesis.client;

import org.apache.hadoop.hdfs.protocol.proto.DataTransferProtos;

import com.bushpath.anamnesis.datatransfer.BlockInputStream;
import com.bushpath.anamnesis.datatransfer.DataTransferProtocol;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.InputStream;
import java.io.IOException;
import java.net.Socket;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

public class AnamnesisInputStream extends InputStream {
    private AnamnesisClient client;
    private List<Block> blocks;
    private int blockIndex;
    private DataInputStream in;
    private DataOutputStream out;
    private int blockSize;

    private byte[] buffer;
    private int startIndex, endIndex;

    public AnamnesisInputStream(AnamnesisClient client, List<Block> blocks, 
            int blockSize) throws IOException {
        this.client = client;
        this.blocks = blocks;
        this.blockIndex = 0;

        this.buffer = new byte[blockSize];
        this.startIndex = 0;
        this.endIndex = 0;

        // sort blocks by offest
        Collections.sort(this.blocks, new Comparator() {
            @Override
            public int compare(Object a, Object b) {
                long offsetA = ((Block)a).getOffset();
                long offsetB = ((Block)b).getOffset();

                if (offsetA > offsetB) {
                    return 1;
                } else if (offsetB > offsetA) {
                    return -1;
                }
                return 0;
            }
        });
    }
    
    @Override
    public int read() throws IOException {
        // if no data in buffer read next block
        if (this.startIndex >= this.endIndex) {
            if (this.readBlock() == 0) {
                return 0;
            }
        }

        int value = this.buffer[this.startIndex];
        this.startIndex += 1;
        return value;
    }

    @Override
    public int read(byte[] b, int off, int len) throws IOException {
        System.out.println("1:anemnesis read " + len + " bytes");
        int bytesRead = 0;
        while (bytesRead < len) {
            System.out.println("1:bytes read - " + bytesRead);
            // if no data in buffer read next block
            if (this.startIndex >= this.endIndex) {
                if (this.readBlock() == 0) {
                    break;
                }
            }

            int copyLen = Math.min(this.endIndex - this.startIndex, len - bytesRead);
            System.arraycopy(this.buffer, this.startIndex, b, off + bytesRead, copyLen);
            bytesRead += copyLen;
            this.startIndex += copyLen;

            /*// write data to buffer
            System.out.println("1.1: " + this.startIndex + "," + this.endIndex);
            int pseudoEndIndex = Math.min(this.endIndex,
                this.startIndex + (len - bytesRead));
            System.out.println("1.2: " + pseudoEndIndex);
            System.arraycopy(this.buffer, this.startIndex, b, off + bytesRead,
                pseudoEndIndex - this.startIndex);
            bytesRead += pseudoEndIndex - this.startIndex;
            this.startIndex += pseudoEndIndex - this.startIndex;
            System.out.println("1.3: " + this.startIndex + "," + this.endIndex);*/
        }

        return bytesRead;
    }

    private int readBlock() throws IOException {
        if (this.blockIndex >= this.blocks.size()) {
            return 0;
        }

        Block block = this.blocks.get(this.blockIndex);
        this.blockIndex += 1;

        System.out.println("BLOCK: " + block.getBlockId());
        for (Location location: block.getLocations()) {
            Socket socket = new Socket(location.getIpAddr(),
                location.getPort());

            this.out = new DataOutputStream(socket.getOutputStream());
            this.in = new DataInputStream(socket.getInputStream());

            // send read op
            DataTransferProtocol.sendReadOp(this.out, "POOL_ID",
                block.getBlockId(), -1, "CLIENT", -1, -1);

            DataTransferProtos.BlockOpResponseProto response =
                DataTransferProtocol.recvBlockOpResponse(in);

            // read data
            this.startIndex = 0;
            this.endIndex = 0;
            int bytesRead = 0;
            BlockInputStream blockIn = new BlockInputStream(in);
            while ((bytesRead = blockIn.read(this.buffer, this.endIndex, 
                    this.buffer.length - this.endIndex)) != 0) {

                this.endIndex += bytesRead;
            }

            for (int i=startIndex; i<endIndex; i++) {
                System.out.print(" " + this.buffer[i]);
            }
            System.out.println();
            // close streams
            blockIn.close();
            in.close();
            out.close();
            socket.close();

            // if successful break
            System.out.println("\tread " + this.endIndex + " total bytes");
            return this.endIndex;
        }

        return 0;
    }

    @Override
    public void close() throws IOException {

    }
}
