package com.bushpath.anamnesis.namenode.protocol;

import com.google.protobuf.ByteString;
import io.grpc.stub.StreamObserver;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolGrpc;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos;
import org.apache.hadoop.hdfs.protocol.proto.HdfsProtos;
import org.apache.hadoop.security.proto.SecurityProtos;

import com.bushpath.anamnesis.namenode.Block;
import com.bushpath.anamnesis.namenode.BlockManager;
import com.bushpath.anamnesis.namenode.DatanodeManager;
import com.bushpath.anamnesis.namenode.NameSystem;
import com.bushpath.anamnesis.namenode.NameSystemFile;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.logging.Logger;

public class ClientNamenodeService
    extends ClientNamenodeProtocolGrpc.ClientNamenodeProtocolImplBase {
    private static final Logger logger =
        Logger.getLogger(ClientNamenodeService.class.getName());

    private BlockManager blockManager;
    private DatanodeManager datanodeManager;
    private NameSystem nameSystem;

    public ClientNamenodeService(BlockManager blockManager,
            DatanodeManager datanodeManager, NameSystem nameSystem) {
        this.blockManager = blockManager;
        this.datanodeManager = datanodeManager;
        this.nameSystem = nameSystem;
    }
    
    @Override
    public void addBlock(ClientNamenodeProtocolProtos.AddBlockRequestProto req,
            StreamObserver<ClientNamenodeProtocolProtos.AddBlockResponseProto>
            responseObserver) {
        logger.info("recv add block request");

        // determine location of block
        long blockId = -1l, generationStamp = -1l, offset = -1l;
        List<HdfsProtos.DatanodeInfoProto> locs = new ArrayList<>();
        List<Boolean> isCached = new ArrayList<>();
        List<HdfsProtos.StorageTypeProto> storageTypes = new ArrayList<>();
        List<String> storageIds = new ArrayList<>();

        try {
            // generate block
            Block block = this.blockManager.createBlock(req.getSrc(), 
                req.getFavoredNodesList());

            // retreive block parameters
            blockId = block.blockId;
            generationStamp = block.generationStamp;
            offset = block.offset;
            locs = block.locs;
        }  catch(Exception e) {
            logger.severe(e.toString());
        }

        // create block protobufs
        HdfsProtos.ExtendedBlockProto b = HdfsProtos.ExtendedBlockProto.newBuilder()
            .setPoolId("")
            .setBlockId(blockId)
            .setGenerationStamp(generationStamp)
            .build();

        SecurityProtos.TokenProto blockToken = SecurityProtos.TokenProto.newBuilder()
            .setIdentifier(ByteString.copyFrom(new byte[]{}))
            .setPassword(ByteString.copyFrom(new byte[]{}))
            .setKind("")
            .setService("")
            .build();

        HdfsProtos.LocatedBlockProto block = HdfsProtos.LocatedBlockProto.newBuilder()
            .setB(b)
            .setOffset(offset)
            .addAllLocs(locs)
            .setCorrupt(false)
            .setBlockToken(blockToken)
            .addAllIsCached(isCached)
            .addAllStorageTypes(storageTypes)
            .addAllStorageIDs(storageIds)
            .build();

        // respond to request
        ClientNamenodeProtocolProtos.AddBlockResponseProto response =
            ClientNamenodeProtocolProtos.AddBlockResponseProto.newBuilder()
                .setBlock(block)
                .build();

        responseObserver.onNext(response);
        responseObserver.onCompleted();
    }

    @Override
    public void complete(ClientNamenodeProtocolProtos.CompleteRequestProto req,
            StreamObserver<ClientNamenodeProtocolProtos.CompleteResponseProto>
            responseObserver) {
        logger.info("recv complete request");

        // complete file with name system
        boolean result = true;
        try {
            // TODO - complete file
        } catch (Exception e) {
            logger.severe(e.toString());
            result = false;
        }

        // response to request
        ClientNamenodeProtocolProtos.CompleteResponseProto response =
            ClientNamenodeProtocolProtos.CompleteResponseProto.newBuilder()
                .setResult(result)
                .build();

        responseObserver.onNext(response);
        responseObserver.onCompleted();
    }

    @Override
    public void create(ClientNamenodeProtocolProtos.CreateRequestProto req,
            StreamObserver<ClientNamenodeProtocolProtos.CreateResponseProto>
            responseObserver) {
        logger.info("recv create request");

        // create file with name system
        try {
            this.nameSystem.create(req.getSrc(), req.getMasked().getPerm(),
                req.getClientName(), req.getCreateParent(), req.getBlockSize());
        } catch (Exception e) {
            logger.severe(e.toString());
        }

        // respond to request
        ClientNamenodeProtocolProtos.CreateResponseProto response =
            ClientNamenodeProtocolProtos.CreateResponseProto.newBuilder()
                .build();

        responseObserver.onNext(response);
        responseObserver.onCompleted();
    }

    @Override
    public void getBlockLocations(
            ClientNamenodeProtocolProtos.GetBlockLocationsRequestProto req,
            StreamObserver<ClientNamenodeProtocolProtos.GetBlockLocationsResponseProto>
            responseObserver) {

        try {
            // look up file
            NameSystemFile file = this.nameSystem.getFile(req.getSrc());

            // retrieve block information
            List<HdfsProtos.LocatedBlockProto> blocks = new ArrayList<>();
            for (Long blockId: file.blocks) {
                Block block = this.blockManager.getBlock(blockId);

                blocks.add(block.toLocatedBlockProto());
            }

            HdfsProtos.LocatedBlocksProto locations =
                HdfsProtos.LocatedBlocksProto.newBuilder()
                    .setFileLength(file.length)
                    .addAllBlocks(blocks)
                    .setUnderConstruction(false) // TODO
                    .setIsLastBlockComplete(false) // TODO
                    .build();

            ClientNamenodeProtocolProtos.GetBlockLocationsResponseProto response
                = ClientNamenodeProtocolProtos.GetBlockLocationsResponseProto.newBuilder()
                    .setLocations(locations)
                    .build();

            responseObserver.onNext(response);
        } catch (Exception e) {
            logger.severe(e.toString());
        }

        responseObserver.onCompleted();
    }

    @Override
    public void getListing(ClientNamenodeProtocolProtos.GetListingRequestProto req,
            StreamObserver<ClientNamenodeProtocolProtos.GetListingResponseProto>
            responseObserver) {
        logger.info("recv get listing request");
        
        // query name system for files
        Collection<NameSystemFile> files = null;
        try {
            files = this.nameSystem.getListing(req.getSrc());
        } catch (Exception e) {
            logger.severe(e.toString());
            files = new ArrayList<>();
        }
    
        // convert NameSystemFile to HdfsFileStatusProto
        List<HdfsProtos.HdfsFileStatusProto> list = new ArrayList<>();
        for (NameSystemFile file: files) {
            HdfsProtos.FsPermissionProto permission = 
                HdfsProtos.FsPermissionProto.newBuilder()
                    .setPerm(file.perm)
                    .build();

            HdfsProtos.HdfsFileStatusProto.FileType fileType = 
                file.file ? HdfsProtos.HdfsFileStatusProto.FileType.IS_FILE : 
                HdfsProtos.HdfsFileStatusProto.FileType.IS_DIR;

            HdfsProtos.HdfsFileStatusProto fileProto = 
                HdfsProtos.HdfsFileStatusProto.newBuilder()
                    .setFileType(fileType)
                    .setPath(ByteString.copyFrom(file.name.getBytes()))
                    .setLength(file.length)
                    .setPermission(permission)
                    .setOwner(file.owner)
                    .setGroup(file.group)
                    .setModificationTime(file.modificationTime)
                    .setAccessTime(file.accessTime)
                    .build();

            list.add(fileProto);
        }


        HdfsProtos.DirectoryListingProto directoryListingProto = 
            HdfsProtos.DirectoryListingProto.newBuilder()
                .addAllPartialListing(list)
                .setRemainingEntries(0)
                .build();

        // respond to request
        ClientNamenodeProtocolProtos.GetListingResponseProto response =
            ClientNamenodeProtocolProtos.GetListingResponseProto.newBuilder()
                .setDirList(directoryListingProto)
                .build();

        responseObserver.onNext(response);
        responseObserver.onCompleted();
    }

    @Override
    public void mkdirs(ClientNamenodeProtocolProtos.MkdirsRequestProto req,
            StreamObserver<ClientNamenodeProtocolProtos.MkdirsResponseProto>
            responseObserver) {
        logger.info("recv create directory request");

        // use name system to make directory
        boolean result = true;
        try {
            this.nameSystem.mkdir(req.getSrc(), req.getMasked().getPerm(),
                req.getCreateParent());
        } catch (Exception e) {
            logger.severe(e.toString());
            result = false;
        }

        // respond to request
        ClientNamenodeProtocolProtos.MkdirsResponseProto response =
            ClientNamenodeProtocolProtos.MkdirsResponseProto.newBuilder()
                .setResult(result)
                .build();

        responseObserver.onNext(response);
        responseObserver.onCompleted();
    }
}
