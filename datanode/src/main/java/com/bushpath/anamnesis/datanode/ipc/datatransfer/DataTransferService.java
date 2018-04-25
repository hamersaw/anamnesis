package com.bushpath.anamnesis.datanode.ipc.datatransfer;

import org.apache.hadoop.hdfs.protocol.proto.DataTransferProtos;
import org.apache.hadoop.hdfs.protocol.proto.HdfsProtos;

import com.bushpath.anamnesis.datanode.inflator.ByteInflator;
import com.bushpath.anamnesis.datanode.inflator.CSVInflator;
import com.bushpath.anamnesis.datanode.inflator.Inflator;
import com.bushpath.anamnesis.datanode.storage.RawBlock;
import com.bushpath.anamnesis.datanode.storage.StatisticsBlock;
import com.bushpath.anamnesis.datanode.storage.Storage;
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

public class DataTransferService extends Thread {
    private int port;
    private Storage storage;

    public DataTransferService(int port, Storage storage) {
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
                new Thread(new DataTransferWorker(socket)).start();
            }
        } catch (Exception e) {
            System.err.println("Unknown server socket error: " + e.toString());
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

                    switch(op) {
                    case WRITE_BLOCK:
                        // recv write op
                        DataTransferProtos.OpWriteBlockProto writeBlockProto =
                            DataTransferProtocol.recvWriteOp(in);

                        HdfsProtos.ExtendedBlockProto extendedBlockProto = 
                            writeBlockProto.getHeader().getBaseHeader().getBlock();

                        System.out.println(System.currentTimeMillis() + ": writing block "
                            + extendedBlockProto.getBlockId());

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
                        int totalBytesRead = 0;

                        ByteArrayOutputStream blockStream = new ByteArrayOutputStream();
                        while ((bytesRead = blockIn.read(buffer)) != 0) {
                            // add bytes to array
                            blockStream.write(buffer, 0, bytesRead);
                            totalBytesRead += bytesRead;
                        }

                        // store block in storage
                        storage.storeBlock(
                            new RawBlock(
                                extendedBlockProto.getBlockId(),
                                extendedBlockProto.getGenerationStamp(),
                                blockStream.toByteArray()
                            ));

                        blockIn.close();

                        System.out.println(System.currentTimeMillis() + ": wrote "
                            + totalBytesRead + " bytes to block "
                            + extendedBlockProto.getBlockId());

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

                        System.out.println(System.currentTimeMillis()
                            + ": writing stats block "
                            + writeBlockStatsProto.getBlockId());

                        // initialize inflator
                        Inflator inflator = null;
                        switch(writeBlockStatsProto.getInflationType()) {
                        case BYTE:
                            inflator = new ByteInflator();
                            break;
                        case CSV:
                            inflator = new CSVInflator();
                            break;
                        }

                        // convert block stats
                        int statsCount = writeBlockStatsProto.getStatisticsCount();
                        double[][] means = new double[statsCount][];
                        double[][] standardDeviations = new double[statsCount][];
                        long[] recordCounts = new long[statsCount];
                            
                        int index = 0;
                        for (DatanodeSketchProtocolProtos.StatisticsProto statisticsProto
                                : writeBlockStatsProto.getStatisticsList()) {

                            List<Double> meansList = statisticsProto.getMeansList();
                            double[] meansArray = new double[meansList.size()];
                            for (int i=0; i<meansList.size(); i++) {
                                meansArray[i] = meansList.get(i).doubleValue();
                            }

                            List<Double> standardDeviationsList =
                                statisticsProto.getStandardDeviationsList();
                            double[] standardDeviationsArray =
                                new double[standardDeviationsList.size()];
                            for (int i=0; i<standardDeviationsList.size(); i++) {
                                standardDeviationsArray[i] =
                                    standardDeviationsList.get(i).doubleValue();
                            }

                            means[index] = meansArray;
                            standardDeviations[index] = standardDeviationsArray;
                            recordCounts[index] = statisticsProto.getRecordCount();
                            index += 1;
                        }

                        storage.storeBlock(
                            new StatisticsBlock(
                                writeBlockStatsProto.getBlockId(),
                                writeBlockStatsProto.getGenerationStamp(),
                                means, standardDeviations, recordCounts, inflator
                            ));

                        System.out.println(System.currentTimeMillis() + ": wrote "
                            + index + " micro-sketches to block "
                            + writeBlockStatsProto.getBlockId());

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

                        System.out.println(System.currentTimeMillis() + ": reading block "
                            + readExtendedBlockProto.getBlockId());

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
                            readChecksum, readBlockProto.getOffset() + 1, false);
                        byte[] readBlock =
                            storage.getBlockBytes(readExtendedBlockProto.getBlockId());
                        blockOut.write(readBlock, (int) readBlockProto.getOffset(),
                            (int) readBlockProto.getLen());
                        blockOut.close();

                        System.out.println(System.currentTimeMillis() + ": read "
                            + readBlockProto.getLen() + " bytes from block " +
                            readExtendedBlockProto.getBlockId());

                        // TODO - read client read status proto
                        DataTransferProtos.ClientReadStatusProto readProto =
                            DataTransferProtocol.recvClientReadStatus(in);

                        break;
                    }
                }
            } catch (EOFException e) {
                // socket was closed by client
            } catch (Exception e) {
                System.out.println("Unknown data transfer error: " + e.toString());
            }
        }
    }
}
