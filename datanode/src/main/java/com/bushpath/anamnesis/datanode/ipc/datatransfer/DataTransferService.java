package com.bushpath.anamnesis.datanode.ipc.datatransfer;

import org.apache.hadoop.hdfs.protocol.proto.DataTransferProtos;
import org.apache.hadoop.hdfs.protocol.proto.HdfsProtos;

import com.bushpath.anamnesis.datanode.storage.Storage;
import com.bushpath.anamnesis.datanode.inflator.Inflator;
import com.bushpath.anamnesis.ipc.datatransfer.BlockInputStream;
import com.bushpath.anamnesis.ipc.datatransfer.BlockOutputStream;
import com.bushpath.anamnesis.ipc.datatransfer.DataTransferProtocol;
import com.bushpath.anamnesis.ipc.datatransfer.Op;
import com.bushpath.anamnesis.checksum.Checksum;
import com.bushpath.anamnesis.checksum.ChecksumFactory;
import com.bushpath.anamensis.protocol.proto.DatanodeSketchProtocolProtos;

import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.List;
import java.util.logging.Logger;

public class DataTransferService extends Thread {
    private static final Logger logger =
        Logger.getLogger(DataTransferService.class.getName());
    private int port;
    private Inflator inflator;
    private Storage storage;

    public DataTransferService(int port, Inflator inflator, Storage storage) {
        this.port = port;
        this.inflator = inflator;
        this.storage = storage;
    }

    @Override
    public void run() {
        try {
            // accept connections on given port and spawn new workers
            ServerSocket serverSocket = new ServerSocket(port);
            while(true) {
                Socket socket = serverSocket.accept();
                new Thread(new DataTransferWorker(socket)).start();
            }
        } catch (Exception e) {
            e.printStackTrace();
            logger.severe(e.toString());
        }
    }

    private class DataTransferWorker extends Thread {
        private Socket socket;

        public DataTransferWorker(Socket socket) {
            this.socket  = socket;
        }

        @Override
        public void run() {
            try {
                DataInputStream in = new DataInputStream(this.socket.getInputStream());
                DataOutputStream out = 
                    new DataOutputStream(this.socket.getOutputStream());
 
                while (true) {
                    // read operation
                    Op op = DataTransferProtocol.readOp(in);
                    logger.info("recv op '" + op + "'");

                    switch(op) {
                    case WRITE_BLOCK:
                        // recv write op
                        DataTransferProtos.OpWriteBlockProto writeBlockProto =
                            DataTransferProtocol.recvWriteOp(in);

                        HdfsProtos.ExtendedBlockProto extendedBlockProto = 
                            writeBlockProto.getHeader().getBaseHeader().getBlock();

                        DataTransferProtos.ChecksumProto writeChecksumProto =
                            writeBlockProto.getRequestedChecksum();

                        // send op response
                        DataTransferProtocol.sendBlockOpResponse(out,
                                DataTransferProtos.Status.SUCCESS);

                        Checksum writeChecksum =
                            ChecksumFactory.buildChecksum(writeChecksumProto.getType());

                        // recv stream block chunks
                        BlockInputStream blockIn = new BlockInputStream(in, out,
                            writeChecksum);
                        byte[] buffer = new byte[1024]; // TODO - find ideal size
                        int bytesRead = 0;

                        ByteArrayOutputStream blockStream = new ByteArrayOutputStream();
                        while ((bytesRead = blockIn.read(buffer)) != 0) {
                            // add bytes to array
                            blockStream.write(buffer, 0, bytesRead);
                        }

                        // store block in storage
                        storage.storeBlock(extendedBlockProto.getBlockId(),
                            blockStream.toByteArray(),
                            extendedBlockProto.getGenerationStamp());

                        blockIn.close();

                        // TODO - fix this
                        /*switch (writeBlockProto.getStage()) {
                        case PIPELINE_CLOSE:
                            return;
                        default:
                            break;
                        }*/

                        break;
                    case WRITE_BLOCK_STATS:
                        // recv write block stats op
                        DatanodeSketchProtocolProtos.WriteBlockStatsProto
                            writeBlockStatsProto = DatanodeSketchProtocolProtos
                                .WriteBlockStatsProto.parseDelimitedFrom(in);

                        ByteArrayOutputStream statisticsBlockOut =
                            new ByteArrayOutputStream();
                        for (DatanodeSketchProtocolProtos.StatisticsProto statisticsProto
                                : writeBlockStatsProto.getStatisticsList()) {

                            // inflate stats
                            List<Double> meansList = statisticsProto.getMeansList();
                            double[] means = new double[meansList.size()];
                            for (int i=0; i<meansList.size(); i++) {
                                means[i] = meansList.get(i).doubleValue();
                            }

                            List<Double> standardDeviationsList =
                                statisticsProto.getStandardDeviationsList();
                            double[] standardDeviations =
                                new double[standardDeviationsList.size()];
                            for (int i=0; i<standardDeviationsList.size(); i++) {
                                standardDeviations[i] =
                                    standardDeviationsList.get(i).doubleValue();
                            }

                            byte[] bytes = inflator.inflate(means, standardDeviations,
                                statisticsProto.getRecordCount());

                            statisticsBlockOut.write(bytes);
                        }

                        byte[] block = statisticsBlockOut.toByteArray();
                        storage.storeBlock(writeBlockStatsProto.getBlockId(), block,
                            writeBlockStatsProto.getGenerationStamp());

                        // send op reponse
                        DataTransferProtocol.sendBlockOpResponse(out,
                                DataTransferProtos.Status.SUCCESS);

                        break;
                    case READ_BLOCK:
                        // recv read op
                        DataTransferProtos.OpReadBlockProto readBlockProto =
                            DataTransferProtocol.recvReadOp(in);

                        HdfsProtos.ExtendedBlockProto readExtendedBlockProto =
                            readBlockProto.getHeader().getBaseHeader().getBlock();

                        // send op response
                        DataTransferProtocol.sendBlockOpResponse(out,
                                DataTransferProtos.Status.SUCCESS,
                                HdfsProtos.ChecksumTypeProto.CHECKSUM_CRC32C, 
                                DataTransferProtocol.CHUNK_SIZE,
                                readBlockProto.getOffset());

                        // create checksum
                        Checksum readChecksum = ChecksumFactory.buildDefaultChecksum();

                        // send stream block chunks
                        BlockOutputStream blockOut = new BlockOutputStream(in, out,
                            readChecksum, readBlockProto.getOffset() + 1);
                        byte[] readBlock =
                            storage.getBlock(readExtendedBlockProto.getBlockId());
                        blockOut.write(readBlock, (int) readBlockProto.getOffset(),
                            (int) readBlockProto.getLen());
                        blockOut.close();

                        // TODO - read client read status proto
                        DataTransferProtos.ClientReadStatusProto readProto =
                            DataTransferProtocol.recvClientReadStatus(in);
                        System.out.println("\tSTATUS:" + readProto.getStatus());

                        break;
                    }
                }
            } catch (EOFException e) {
                // socket was closed by client
            } catch (Exception e) {
                e.printStackTrace();
                logger.severe(e.toString());
            }
        }
    }
}
