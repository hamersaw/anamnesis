package com.bushpath.anamnesis.namenode.protocol;

import com.google.protobuf.ByteString;
import io.grpc.stub.StreamObserver;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolGrpc;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos;
import org.apache.hadoop.hdfs.protocol.proto.HdfsProtos;

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

    private DatanodeManager datanodeManager;
    private NameSystem nameSystem;

    public ClientNamenodeService(DatanodeManager datanodeManager,
            NameSystem nameSystem) {
        this.datanodeManager = datanodeManager;
        this.nameSystem = nameSystem;
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
