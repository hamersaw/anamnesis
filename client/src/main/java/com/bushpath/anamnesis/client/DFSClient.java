package com.bushpath.anamnesis.client;

import com.google.protobuf.ByteString;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos;
import org.apache.hadoop.hdfs.protocol.proto.HdfsProtos;

import com.bushpath.anamnesis.client.protocol.ClientNamenodeClient;

import java.util.ArrayList;
import java.util.List;
import java.util.logging.Logger;

public class DFSClient {
    private static final Logger logger = Logger.getLogger(DFSClient.class.getName());
    private static final long BLOCK_SIZE = 64000;
    private ClientNamenodeClient clientNamenodeClient;

    public DFSClient(String ipAddr, int port) {
        this.clientNamenodeClient = new ClientNamenodeClient(ipAddr, port);
    }

    public void create(String path) {
        logger.info("creating file '" + path + "'");

        // construct protobuf components
        HdfsProtos.FsPermissionProto fsPermissionProto = 
            HdfsProtos.FsPermissionProto.newBuilder()
                .setPerm(Integer.MAX_VALUE)
                .build();

        List<HdfsProtos.CryptoProtocolVersionProto> cryptoProtocolVersion
            = new ArrayList<>();
        cryptoProtocolVersion.add(
                HdfsProtos.CryptoProtocolVersionProto.UNKNOWN_PROTOCOL_VERSION);

        ClientNamenodeProtocolProtos.CreateRequestProto req = 
            ClientNamenodeProtocolProtos.CreateRequestProto.newBuilder()
                .setSrc(path)
                .setMasked(fsPermissionProto)
                .setClientName("DFSClient")
                .setCreateFlag(Integer.MAX_VALUE)
                .setCreateParent(true)
                .setReplication(-1)
                .setBlockSize(this.BLOCK_SIZE)
                .addAllCryptoProtocolVersion(cryptoProtocolVersion)
                .build();

        // send CreateRequestProto
        ClientNamenodeProtocolProtos.CreateResponseProto response =
            this.clientNamenodeClient.create(req);

        // TODO - handle response
    }

    public void ls(String path) {
        logger.info("get listings for '" + path + "'");
        //this.clientNamenodeClient.getListing(path, new byte[]{}, false);
        // construct protobuf components
        ClientNamenodeProtocolProtos.GetListingRequestProto req =
            ClientNamenodeProtocolProtos.GetListingRequestProto.newBuilder()
                .setSrc(path)
                .setStartAfter(ByteString.copyFrom(new byte[]{}))
                .setNeedLocation(false)
                .build();

        // send GetListingRequestProto
        ClientNamenodeProtocolProtos.GetListingResponseProto response =
            this.clientNamenodeClient.getListing(req);

        // TODO - handle response
        List<HdfsProtos.HdfsFileStatusProto> list = response.getDirList()
            .getPartialListingList();

        for (HdfsProtos.HdfsFileStatusProto file: list) {
            System.out.println(file.getFileType() + ":" + file.getModificationTime());
        }
    }

    public void mkdir(String path) {
        logger.info("creating directory '" + path + "'");
 
        // construct protobuf components
        HdfsProtos.FsPermissionProto fsPermissionProto =
            HdfsProtos.FsPermissionProto.newBuilder()
                .setPerm(Integer.MAX_VALUE)
                .build();

        ClientNamenodeProtocolProtos.MkdirsRequestProto req = 
            ClientNamenodeProtocolProtos.MkdirsRequestProto.newBuilder()
                .setSrc(path)
                .setMasked(fsPermissionProto)
                .setCreateParent(true)
                .build();

        // send MkdirsRequestProto
        ClientNamenodeProtocolProtos.MkdirsResponseProto response =
            this.clientNamenodeClient.mkdirs(req);

        // TODO - handle response
    }
}
