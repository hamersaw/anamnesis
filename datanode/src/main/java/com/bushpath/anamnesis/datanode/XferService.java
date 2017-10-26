package com.bushpath.anamnesis.datanode;

import org.apache.hadoop.hdfs.protocol.proto.DataTransferProtos;
import org.apache.hadoop.hdfs.protocol.proto.HdfsProtos;

import com.bushpath.anamnesis.datatransfer.BlockInputStream;
import com.bushpath.anamnesis.datatransfer.DataTransferProtocol;
import com.bushpath.anamnesis.datatransfer.Op;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.logging.Logger;

public class XferService extends Thread {
    private static final Logger logger = Logger.getLogger(XferService.class.getName());
    private int port;

    public XferService(int port) {
        this.port = port;
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
                        while ((bytesRead = blockIn.read(buffer)) != 0) {
                            // TODO - send to file system
                            System.out.println("\trecv " + bytesRead + " bytes");
                        }

                        blockIn.close();

                        switch (writeBlockProto.getStage()) {
                        case PIPELINE_CLOSE:
                            return;
                        default:
                            break;
                        }

                        break;
                    case READ_BLOCK:
                        DataTransferProtocol.recvReadOp(in);
                        // TODO - send stream block chunks
                        break;
                    }
                }
            } catch (Exception e) {
                e.printStackTrace();
                logger.severe(e.toString());
            }
        }
    }
}
