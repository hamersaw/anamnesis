package com.bushpath.anamnesis.datanode;

import org.apache.hadoop.hdfs.protocol.proto.DataTransferProtos;

import com.bushpath.anamnesis.DataTransferProtocol;
import com.bushpath.anamnesis.Op;

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
                    logger.info("recv op " + op);

                    DataTransferProtos.Status status = DataTransferProtos.Status.SUCCESS;
                    switch(op) {
                    case WRITE_BLOCK:
                        DataTransferProtocol.recvWriteOp(in);
                        break;
                    case READ_BLOCK:
                        DataTransferProtocol.recvReadOp(in);
                        break;
                    }

                    DataTransferProtocol.sendBlockOpResponse(out, status);

                    // TODO - recv stream block chunks
                }
            } catch (Exception e) {
                logger.severe(e.toString());
            }
        }
    }
}
