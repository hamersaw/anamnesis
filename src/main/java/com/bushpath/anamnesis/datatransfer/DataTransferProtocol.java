package com.bushpath.anamnesis.datatransfer;

import com.google.protobuf.Message;
import org.apache.hadoop.hdfs.protocol.proto.DataTransferProtos;
import org.apache.hadoop.hdfs.protocol.proto.HdfsProtos;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

public class DataTransferProtocol {
    public static final int PROTOCOL_VERSION = 28;

    public static void sendBlockOpResponse(DataOutputStream out,
            DataTransferProtos.Status status) throws IOException {

        DataTransferProtos.BlockOpResponseProto blockOpResponseProto =
            DataTransferProtos.BlockOpResponseProto.newBuilder()
                .setStatus(status)
                .build();

        blockOpResponseProto.writeDelimitedTo(out);
        out.flush();
    }

    public static void sendReadOp(DataOutputStream out) throws IOException {
        // TODO - build read op

        send(out, Op.READ_BLOCK, null);
    }

    public static void sendWriteOp(DataOutputStream out, 
            DataTransferProtos.OpWriteBlockProto.BlockConstructionStage stage, 
            String poolId, long blockId, long generationStamp, String client) 
            throws IOException {
        HdfsProtos.ExtendedBlockProto extendedBlockProto =
            HdfsProtos.ExtendedBlockProto.newBuilder()
                .setPoolId(poolId)
                .setBlockId(blockId)
                .setGenerationStamp(generationStamp)
                .build();

        DataTransferProtos.BaseHeaderProto baseHeaderProto =
            DataTransferProtos.BaseHeaderProto.newBuilder()
                .setBlock(extendedBlockProto)
                .build();

        DataTransferProtos.ClientOperationHeaderProto clientOperationHeaderProto =
            DataTransferProtos.ClientOperationHeaderProto.newBuilder()
                .setBaseHeader(baseHeaderProto)
                .setClientName(client)
                .build();

        DataTransferProtos.ChecksumProto checksumProto =
            DataTransferProtos.ChecksumProto.newBuilder()
                .setType(HdfsProtos.ChecksumTypeProto.CHECKSUM_NULL)
                .setBytesPerChecksum(-1)
                .build();

        Message proto = DataTransferProtos.OpWriteBlockProto.newBuilder()
            .setHeader(clientOperationHeaderProto)
            .setStage(stage)
            .setRequestedChecksum(checksumProto)
            .setPipelineSize(-1) // TODO - all these variables
            .setMinBytesRcvd(-1l)
            .setMaxBytesRcvd(-1l)
            .setLatestGenerationStamp(-1l)
            .build();

        send(out, Op.WRITE_BLOCK, proto);
    }

    private static void send(DataOutputStream out, Op op, Message proto)
            throws IOException {
        out.writeShort(PROTOCOL_VERSION);
        op.write(out);
        proto.writeDelimitedTo(out);
        out.flush();
    }

    public static Op readOp(DataInputStream in) throws IOException {
        int version = in.readShort();
        if (version != PROTOCOL_VERSION) {
            throw new IOException("invalid data transfer protocol version");
        }

        return Op.read(in);
    }

    public static DataTransferProtos.BlockOpResponseProto
            recvBlockOpResponse(DataInputStream in) throws IOException {

        return DataTransferProtos.BlockOpResponseProto.parseDelimitedFrom(in);
    }

    public static DataTransferProtos.OpReadBlockProto
            recvReadOp(DataInputStream in) throws IOException {

        return DataTransferProtos.OpReadBlockProto.parseDelimitedFrom(in);
    }

    public static DataTransferProtos.OpWriteBlockProto
            recvWriteOp(DataInputStream in) throws IOException {

        return DataTransferProtos.OpWriteBlockProto.parseDelimitedFrom(in);
    }
}
