package com.bushpath.anamnesis.client.protocol;

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;

import com.bushpath.anamnesis.protocol.ClientNamenodeProtocol;
import com.bushpath.anamnesis.protocol.Hdfs;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolGrpc;
import org.apache.hadoop.hdfs.protocol.proto.HdfsProtos;

import java.util.ArrayList;
import java.util.List;
import java.util.logging.Logger;

public class ClientNamenodeClient {
    private static final Logger logger = 
        Logger.getLogger(ClientNamenodeClient.class.getName());

    // grpc variables
    private final ManagedChannel channel;
    private final 
        ClientNamenodeProtocolGrpc.ClientNamenodeProtocolBlockingStub 
        blockingStub;
    
    public ClientNamenodeClient(String host, int port) {
        // construct channel and initialize blocking stub
        this.channel = ManagedChannelBuilder.forAddress(host, port)
                        .usePlaintext(true)
                        .build();

        this.blockingStub = ClientNamenodeProtocolGrpc.newBlockingStub(channel);
    }

    public void create(String path, int perm, String clientName, int createFlag,
            boolean createParent, int replication, long blockSize) {

        // construct protobuf components
        HdfsProtos.FsPermissionProto fsPermissionProto = 
            Hdfs.buildFsPermissionProto(perm);

        List<HdfsProtos.CryptoProtocolVersionProto> cryptoProtocolVersion
            = new ArrayList<>();
        cryptoProtocolVersion.add(
                HdfsProtos.CryptoProtocolVersionProto.UNKNOWN_PROTOCOL_VERSION);

        ClientNamenodeProtocolProtos.CreateRequestProto req =
            ClientNamenodeProtocol.buildCreateRequestProto(path, fsPermissionProto,
                clientName, createFlag, createParent, replication, blockSize,
                cryptoProtocolVersion);

        // send CreateRequestProto
        ClientNamenodeProtocolProtos.CreateResponseProto response =
            this.blockingStub.create(req);

        // TODO - handle response
    }

    public void getListing(String path, byte[] startAfter, boolean needLocation) {
        // construct protobuf components
        ClientNamenodeProtocolProtos.GetListingRequestProto req =
            ClientNamenodeProtocol.buildGetListingRequestProto(path, 
                startAfter, needLocation);

        // send GetListingRequestProto
        ClientNamenodeProtocolProtos.GetListingResponseProto response =
            this.blockingStub.getListing(req);

        // TODO - handle response
        List<HdfsProtos.HdfsFileStatusProto> list = response.getDirList()
            .getPartialListingList();

        for (HdfsProtos.HdfsFileStatusProto file: list) {
            System.out.println(file.getFileType() + ":" + file.getModificationTime());
        }
    }

    public void mkdir(String path, int perm, boolean createParent) {
        // construct protobuf components
        HdfsProtos.FsPermissionProto fsPermissionProto = 
            Hdfs.buildFsPermissionProto(perm);

        ClientNamenodeProtocolProtos.MkdirsRequestProto req = 
            ClientNamenodeProtocol.buildMkdirsRequestProto(path, 
                fsPermissionProto, createParent);

        // send MkdirsRequestProto
        ClientNamenodeProtocolProtos.MkdirsResponseProto response =
            this.blockingStub.mkdirs(req);

        // TODO - handle response
    }
}
