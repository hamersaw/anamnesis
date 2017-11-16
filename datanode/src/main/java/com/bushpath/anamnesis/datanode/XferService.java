package com.bushpath.anamnesis.datanode;

import org.apache.hadoop.hdfs.protocol.proto.DataTransferProtos;
import org.apache.hadoop.hdfs.protocol.proto.HdfsProtos;

import com.bushpath.anamnesis.datanode.storage.Storage;
import com.bushpath.anamnesis.datatransfer.BlockInputStream;
import com.bushpath.anamnesis.datatransfer.BlockOutputStream;
import com.bushpath.anamnesis.datatransfer.DataTransferProtocol;
import com.bushpath.anamnesis.datatransfer.Op;
import com.bushpath.anamnesis.util.Checksum;
import com.bushpath.anamnesis.util.ChecksumFactory;

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
            e.printStackTrace();
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
 
                // read operation
                Op op = DataTransferProtocol.readOp(in);
                logger.info("recv op '" + op + "'");

                switch(op) {
                case WRITE_BLOCK:
                    // send op response
                    DataTransferProtocol.sendBlockOpResponse(out,
                            DataTransferProtos.Status.SUCCESS);

                    // recv write op
                    DataTransferProtos.OpWriteBlockProto writeBlockProto =
                        DataTransferProtocol.recvWriteOp(in);

                    HdfsProtos.ExtendedBlockProto extendedBlockProto = 
                        writeBlockProto.getHeader().getBaseHeader().getBlock();

                    DataTransferProtos.ChecksumProto writeChecksumProto =
                        writeBlockProto.getRequestedChecksum();

                    Checksum writeChecksum =
                        ChecksumFactory.buildChecksum(writeChecksumProto.getType());

                    System.out.println("WRITE CHECKSUM TYPE:" + writeChecksumProto.getType() + ":" + writeChecksumProto.getBytesPerChecksum());

                    // recv stream block chunks
                    BlockInputStream blockIn = new BlockInputStream(in, out,
                        writeChecksum);
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

                    // TODO - fix this
                    /*switch (writeBlockProto.getStage()) {
                    case PIPELINE_CLOSE:
                        return;
                    default:
                        break;
                    }*/

                    break;
                case READ_BLOCK:
                    // create checksum
                    Checksum readChecksum = ChecksumFactory.buildDefaultChecksum();

                    // send op response
                    DataTransferProtocol.sendBlockOpResponse(out,
                            DataTransferProtos.Status.SUCCESS,
                            HdfsProtos.ChecksumTypeProto.CHECKSUM_CRC32C, 512, 0l);

                    // recv read op
                    DataTransferProtos.OpReadBlockProto readBlockProto =
                        DataTransferProtocol.recvReadOp(in);
                    System.out.println("RECV READ_BLOCK OP");
                    System.out.println("\toffset:" + readBlockProto.getOffset());
                    System.out.println("\tlen:" + readBlockProto.getLen());
                    System.out.println("\tsendChecksums:"
                        + readBlockProto.getSendChecksums());

                    HdfsProtos.ExtendedBlockProto readExtendedBlockProto =
                        readBlockProto.getHeader().getBaseHeader().getBlock();

                    System.out.println("\tblock id: "
                        + readExtendedBlockProto.getBlockId());
                    
                    // send stream block chunks
                    BlockOutputStream blockOut = new BlockOutputStream(in, out,
                        readChecksum);
                    byte[] readBlock =
                        storage.getBlock(readExtendedBlockProto.getBlockId());
                    blockOut.write(readBlock);
                    blockOut.close();

                    // TODO - read client read status proto
                    System.out.println("READING CLIENT READ STATUS PROTO");
                    DataTransferProtos.ClientReadStatusProto readProto =
                        DataTransferProtocol.recvClientReadStatus(in);
                    System.out.println("\tSTATUS:" + readProto.getStatus());

                    // send op response
                    //DataTransferProtocol.sendBlockOpResponse(out,
                    //        DataTransferProtos.Status.SUCCESS);
                    //in.close();
                    out.close();
                    //this.socket.close();
                    return;
                    //break;
                }
            } catch (Exception e) {
                e.printStackTrace();
                logger.severe(e.toString());
            }
        }
    }
}
