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
import com.bushpath.anamnesis.namenode.NSFile;
import com.bushpath.anamnesis.namenode.NSItem;

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

        try {
            // create block
            Block block = this.blockManager.createBlock(req.getSrc(), 
                req.getFavoredNodesList());

            // respond to request
            ClientNamenodeProtocolProtos.AddBlockResponseProto response =
                ClientNamenodeProtocolProtos.AddBlockResponseProto.newBuilder()
                    .setBlock(block.toLocatedBlockProto())
                    .build();

            responseObserver.onNext(response);
        } catch (Exception e) {
            logger.severe(e.toString());
            responseObserver.onError(e);
        }

        responseObserver.onCompleted();
    }

    @Override
    public void complete(ClientNamenodeProtocolProtos.CompleteRequestProto req,
            StreamObserver<ClientNamenodeProtocolProtos.CompleteResponseProto>
            responseObserver) {
        logger.info("recv complete request");

        try {
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
        } catch (Exception e) {
            logger.severe(e.toString());
            responseObserver.onError(e);
        }

        responseObserver.onCompleted();
    }

    @Override
    public void create(ClientNamenodeProtocolProtos.CreateRequestProto req,
            StreamObserver<ClientNamenodeProtocolProtos.CreateResponseProto>
            responseObserver) {
        logger.info("recv create request");

        try {
            // create file with name system
            this.nameSystem.create(req.getSrc(), req.getMasked().getPerm(),
                req.getClientName(), req.getCreateParent(), req.getBlockSize());

            // respond to request
            ClientNamenodeProtocolProtos.CreateResponseProto response =
                ClientNamenodeProtocolProtos.CreateResponseProto.newBuilder()
                    .build();

            responseObserver.onNext(response);
        } catch (Exception e) {
            logger.severe(e.toString());
            responseObserver.onError(e);
        }

        responseObserver.onCompleted();
    }

    @Override
    public void getBlockLocations(
            ClientNamenodeProtocolProtos.GetBlockLocationsRequestProto req,
            StreamObserver<ClientNamenodeProtocolProtos.GetBlockLocationsResponseProto>
            responseObserver) {

        try {
            // look up file
            NSItem item = this.nameSystem.getFile(req.getSrc());
            if (item.getType() != NSItem.Type.FILE) {
                throw new Exception("file is not of type 'FILE'");
            }
            NSFile file = (NSFile) item;

            // retrieve block information
            List<HdfsProtos.LocatedBlockProto> blocks = new ArrayList<>();
            for (Long blockId: file.getBlocks()) {
                Block block = this.blockManager.getBlock(blockId);

                blocks.add(block.toLocatedBlockProto());
            }

            HdfsProtos.LocatedBlocksProto locations =
                HdfsProtos.LocatedBlocksProto.newBuilder()
                    .setFileLength(file.getLength())
                    .addAllBlocks(blocks)
                    .setUnderConstruction(!file.isComplete())
                    .setIsLastBlockComplete(false) // TODO
                    .build();

            ClientNamenodeProtocolProtos.GetBlockLocationsResponseProto response
                = ClientNamenodeProtocolProtos.GetBlockLocationsResponseProto.newBuilder()
                    .setLocations(locations)
                    .build();

            responseObserver.onNext(response);
        } catch (Exception e) {
            logger.severe(e.toString());
            responseObserver.onError(e);
        }

        responseObserver.onCompleted();
    }

    @Override
    public void getListing(ClientNamenodeProtocolProtos.GetListingRequestProto req,
            StreamObserver<ClientNamenodeProtocolProtos.GetListingResponseProto>
            responseObserver) {
        logger.info("recv get listing request");
        
        // query name system for files
        try {
            Collection<NSItem> items = this.nameSystem.getListing(req.getSrc());
    
            // convert NSItem to HdfsFileStatusProto
            List<HdfsProtos.HdfsFileStatusProto> list = new ArrayList<>();
            for (NSItem item: items) {
                list.add(item.toHdfsFileStatusProto());
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
        } catch (Exception e) {
            logger.severe(e.toString());
            responseObserver.onError(e);
        }

        responseObserver.onCompleted();
    }

    @Override
    public void mkdirs(ClientNamenodeProtocolProtos.MkdirsRequestProto req,
            StreamObserver<ClientNamenodeProtocolProtos.MkdirsResponseProto>
            responseObserver) {
        logger.info("recv create directory request");

        try {
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
        } catch (Exception e) {
            logger.severe(e.toString());
            responseObserver.onError(e);
        }

        responseObserver.onCompleted();
    }
}
