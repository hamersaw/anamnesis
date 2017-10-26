package com.bushpath.anamnesis.datanode;

import org.apache.hadoop.hdfs.protocol.proto.DataTransferProtos;
import org.apache.hadoop.hdfs.protocol.proto.HdfsProtos;

import com.bushpath.anamnesis.datanode.storage.Storage;
import com.bushpath.anamnesis.datatransfer.BlockInputStream;
import com.bushpath.anamnesis.datatransfer.BlockOutputStream;
import com.bushpath.anamnesis.datatransfer.DataTransferProtocol;
import com.bushpath.anamnesis.datatransfer.Op;

import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.logging.Logger;

public class XferService extends Thread {
    private static final Logger logger = Logger.getLogger(XferService.class.getName());
    private int port;
    private Storage storage;

    public XferService(int port, Storage storage) {
        this.port = port;
        this.storage = storage;
    }

    @Override
    public void run() {
        try {
            // accept connections on given port and spawn new workers
            ServerSocket serverSocket = new ServerSocket(port);
            while(true) {
                Socket socket = serverSocket.accept();
                new Thread(new XferWorker(socket)).start();
            }
        } catch (Exception e) {
            logger.severe(e.toString());
        }
    }

    private class XferWorker extends Thread {
        private Socket socket;

        public XferWorker(Socket socket) {
            this.socket  = socket;
        }

        @Override
        public void run() {
            try {
                DataInputStream in = new DataInputStream(this.socket.getInputStream());
                DataOutputStream out = 
                    new DataOutputStream(this.socket.getOutputStream());
                while(true) {
                    // read operation
                    Op op = DataTransferProtocol.readOp(in);
                    logger.info("recv op '" + op + "'");

                    // send op response
                    DataTransferProtos.Status status = DataTransferProtos.Status.SUCCESS;
                    DataTransferProtocol.sendBlockOpResponse(out, status);

                    switch(op) {
                    case WRITE_BLOCK:
                        DataTransferProtos.OpWriteBlockProto writeBlockProto =
                            DataTransferProtocol.recvWriteOp(in);

                        HdfsProtos.ExtendedBlockProto extendedBlockProto = 
                            writeBlockProto.getHeader().getBaseHeader().getBlock();

                        // recv stream block chunks
                        BlockInputStream blockIn = new BlockInputStream(in);
                        byte[] buffer = new byte[1024]; 
                        int bytesRead = 0;

                        ByteArrayOutputStream blockStream = new ByteArrayOutputStream();
                        while ((bytesRead = blockIn.read(buffer)) != 0) {
                            // add bytes to array
                            blockStream.write(buffer, 0, bytesRead);
                        }

                        // store block in storage
                        storage.storeBlock(extendedBlockProto.getBlockId(),
                            blockStream.toByteArray());

                        blockIn.close();

                        switch (writeBlockProto.getStage()) {
                        case PIPELINE_CLOSE:
                            return;
                        default:
                            break;
                        }

                        break;
                    case READ_BLOCK:
                        DataTransferProtos.OpReadBlockProto readBlockProto =
                            DataTransferProtocol.recvReadOp(in);

                        HdfsProtos.ExtendedBlockProto readExtendedBlockProto =
                            readBlockProto.getHeader().getBaseHeader().getBlock();
                        
                        // send stream block chunks
                        BlockOutputStream blockOut = new BlockOutputStream(out);
                        byte[] readBlock =
                            storage.getBlock(readExtendedBlockProto.getBlockId());
                        blockOut.write(readBlock);
                        blockOut.close();

                        return;
                    }
                }
            } catch (Exception e) {
                logger.severe(e.toString());
            }
        }
    }
}
