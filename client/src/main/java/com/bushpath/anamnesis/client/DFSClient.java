package com.bushpath.anamnesis.client;

import com.google.protobuf.ByteString;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos;
import org.apache.hadoop.hdfs.protocol.proto.HdfsProtos;

import com.bushpath.anamnesis.client.protocol.ClientNamenodeClient;

import java.io.FileInputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Logger;

public class DFSClient {
    private static final Logger logger = Logger.getLogger(DFSClient.class.getName());
    private ClientNamenodeClient clientNamenodeClient;
    private String clientName;

    public DFSClient(String ipAddr, int port, String clientName) {
        this.clientNamenodeClient = new ClientNamenodeClient(ipAddr, port);
        this.clientName = clientName;
    }

    public void ls(String path) throws Exception {
        logger.info("get listings for '" + path + "'");

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
            System.out.println(new String(file.getPath().toByteArray())
                    + ":" + file.getFileType() 
                    + ":" + file.getModificationTime());
        }
    }

    public void mkdir(String path) throws Exception {
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

    public void upload(String localPath, String path, int blockSize) throws Exception {
        logger.info("creating file '" + path + "'");

        // open local file input stream
        FileInputStream input = new FileInputStream(localPath);

        // construct protobuf components for creating file
        HdfsProtos.FsPermissionProto fsPermissionProto = 
            HdfsProtos.FsPermissionProto.newBuilder()
                .setPerm(Integer.MAX_VALUE)
                .build();

        List<HdfsProtos.CryptoProtocolVersionProto> cryptoProtocolVersion
            = new ArrayList<>();
        cryptoProtocolVersion.add(
                HdfsProtos.CryptoProtocolVersionProto.UNKNOWN_PROTOCOL_VERSION);

        ClientNamenodeProtocolProtos.CreateRequestProto createReq = 
            ClientNamenodeProtocolProtos.CreateRequestProto.newBuilder()
                .setSrc(path)
                .setMasked(fsPermissionProto)
                .setClientName(this.clientName)
                .setCreateFlag(1)
                .setCreateParent(true)
                .setReplication(-1)
                .setBlockSize(blockSize)
                .addAllCryptoProtocolVersion(cryptoProtocolVersion)
                .build();

        // send CreateRequestProto
        ClientNamenodeProtocolProtos.CreateResponseProto createResponse =
            this.clientNamenodeClient.create(createReq);

        // TODO - handle response

        // parse file and handle blocks individually
        byte[] block = new byte[blockSize];
        int bytesRead;
        while ((bytesRead = input.read(block)) > 0) {
            // create protobuf components for add block request
            ClientNamenodeProtocolProtos.AddBlockRequestProto addBlockReq =
                ClientNamenodeProtocolProtos.AddBlockRequestProto.newBuilder()
                    .setSrc(path)
                    .setClientName(this.clientName)
                    // TODO - add List<String> favoredNodes
                    .build();

            ClientNamenodeProtocolProtos.AddBlockResponseProto addBlockResponse =
                this.clientNamenodeClient.addBlock(addBlockReq);

            // TODO - handle response
            System.out.println("writing block to");
            for (HdfsProtos.DatanodeInfoProto loc: 
                    addBlockResponse.getBlock().getLocsList()) {

                System.out.println("\t" + loc.getId().getIpAddr() + ":" 
                    + loc.getId().getXferPort());
            }
        }
 
        // complete file
        ClientNamenodeProtocolProtos.CompleteRequestProto completeReq =
            ClientNamenodeProtocolProtos.CompleteRequestProto.newBuilder()
                .setSrc(path)
                .setClientName(this.clientName)
                .build();

        ClientNamenodeProtocolProtos.CompleteResponseProto completeResponse =
            this.clientNamenodeClient.complete(completeReq);

        // TODO - handle response
    }

}
