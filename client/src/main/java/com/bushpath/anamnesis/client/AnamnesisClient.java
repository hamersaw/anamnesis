package com.bushpath.anamnesis.client;

import com.google.protobuf.ByteString;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos;
import org.apache.hadoop.hdfs.protocol.proto.HdfsProtos;

import com.bushpath.anamnesis.client.protocol.ClientNamenodeClient;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Logger;

public class AnamnesisClient {
    private static final Logger logger =
        Logger.getLogger(AnamnesisClient.class.getName());
    private ClientNamenodeClient clientNamenodeClient;
    private String clientName;

    public AnamnesisClient(String ipAddr, int port, String clientName) {
        this.clientNamenodeClient = new ClientNamenodeClient(ipAddr, port);
        this.clientName = clientName;
    }

    public List<Location> addBlock(String path,
            List<String> favoredNodes) throws IOException {
        // create protobuf components for add block request
        ClientNamenodeProtocolProtos.AddBlockRequestProto addBlockReq =
            ClientNamenodeProtocolProtos.AddBlockRequestProto.newBuilder()
                .setSrc(path)
                .setClientName(this.clientName)
                .addAllFavoredNodes(favoredNodes)
                .build();

        ClientNamenodeProtocolProtos.AddBlockResponseProto addBlockResponse =
            this.clientNamenodeClient.addBlock(addBlockReq);

        // return locations specified by namenode
        List<Location> locations = new ArrayList<>();
        for (HdfsProtos.DatanodeInfoProto loc: 
                addBlockResponse.getBlock().getLocsList()) {
            locations.add(new Location(loc.getId().getIpAddr(),
                loc.getId().getXferPort()));
        }

        return locations;
    }

    public void close(String path) throws IOException {
        // complete file
        ClientNamenodeProtocolProtos.CompleteRequestProto req =
            ClientNamenodeProtocolProtos.CompleteRequestProto.newBuilder()
                .setSrc(path)
                .setClientName(this.clientName)
                .build();

        ClientNamenodeProtocolProtos.CompleteResponseProto response =
            this.clientNamenodeClient.complete(req);

        // TODO - handle response
    }

    public AnamnesisOutputStream create(String path, int perm, int blockSize,
            List<String> favoredNodes) throws IOException {
        logger.info("creating remote file '" + path + "'");

        // construct protobuf components for creating file
        HdfsProtos.FsPermissionProto fsPermissionProto = 
            HdfsProtos.FsPermissionProto.newBuilder()
                .setPerm(perm)
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

        return new AnamnesisOutputStream(this, path, blockSize, favoredNodes);
    }

    public void download(String path, String localPath) throws IOException {
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
            throw new IOException("directory downloads not yet supported");
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

    public void ls(String path) throws IOException {
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

    public boolean mkdir(String path) throws IOException {
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
        return response.getResult();
    }
}
