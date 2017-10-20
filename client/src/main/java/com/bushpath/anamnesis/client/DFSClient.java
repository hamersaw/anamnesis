package com.bushpath.anamnesis.client;

import com.google.protobuf.ByteString;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos;
import org.apache.hadoop.hdfs.protocol.proto.HdfsProtos;

import com.bushpath.anamnesis.client.protocol.ClientNamenodeClient;

import java.io.FileInputStream;
import java.io.FileOutputStream;
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

    public void download(String path, String localPath) throws Exception {
        logger.info("downloading file '" + path + "' to '" + localPath + "'");

        // open file output stream
        FileOutputStream output = new FileOutputStream(localPath);

        // construct getLsting protobuf components
        ClientNamenodeProtocolProtos.GetListingRequestProto getListingReq =
            ClientNamenodeProtocolProtos.GetListingRequestProto.newBuilder()
                .setSrc(path)
                .setStartAfter(ByteString.copyFrom(new byte[]{}))
                .setNeedLocation(true)
                .build();

        // send GetListingRequestProto
        ClientNamenodeProtocolProtos.GetListingResponseProto getListingResponse =
            this.clientNamenodeClient.getListing(getListingReq);

        // handle response
        List<HdfsProtos.HdfsFileStatusProto> list = getListingResponse.getDirList()
            .getPartialListingList();

        if (list.size() != 1) {
            throw new Exception("directory downloads not yet supported");
        }

        HdfsProtos.HdfsFileStatusProto file = list.get(0);

        // TODO - download block locations
        HdfsProtos.LocatedBlocksProto locatedBlocks = file.getLocations();
        for (HdfsProtos.LocatedBlockProto block: locatedBlocks.getBlocksList()) {
            System.out.println("TODO - download block " + block.getB().getBlockId());

            for (HdfsProtos.DatanodeInfoProto loc: block.getLocsList()) {
                System.out.println("\t" + loc.getId().getIpAddr() + ":" 
                    + loc.getId().getXferPort());
            }
        }
    }

    public void ls(String path) throws Exception {
        logger.info("get listings for '" + path + "'");

        // construct GetListing protobuf components
        ClientNamenodeProtocolProtos.GetListingRequestProto req =
            ClientNamenodeProtocolProtos.GetListingRequestProto.newBuilder()
                .setSrc(path)
                .setStartAfter(ByteString.copyFrom(new byte[]{}))
                .setNeedLocation(false)
                .build();

        // send GetListingRequestProto
        ClientNamenodeProtocolProtos.GetListingResponseProto response =
            this.clientNamenodeClient.getListing(req);

        // handle response
        List<HdfsProtos.HdfsFileStatusProto> list = response.getDirList()
            .getPartialListingList();

        for (HdfsProtos.HdfsFileStatusProto file: list) {
            System.out.println(new String(file.getPath().toByteArray())
                    + ":" + file.getFileType());
        }
    }

    public void mkdir(String path) throws Exception {
        logger.info("creating directory '" + path + "'");
 
        // construct Mkdir protobuf components
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

        // handle response
        System.out.println("Success:" + response.getResult());
    }

    public void upload(String localPath, String path, int blockSize,
            List<String> favoredNodes) throws Exception {
        logger.info("uploading file '" + localPath + "' to '" + path + "'");

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
                    .addAllFavoredNodes(favoredNodes)
                    .build();

            ClientNamenodeProtocolProtos.AddBlockResponseProto addBlockResponse =
                this.clientNamenodeClient.addBlock(addBlockReq);

            // TODO - upload blocks
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
