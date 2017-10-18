package com.bushpath.anamnesis.namenode.protocol;

import io.grpc.stub.StreamObserver;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolGrpc;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos;
import org.apache.hadoop.hdfs.protocol.proto.HdfsProtos;

import com.bushpath.anamnesis.namenode.DatanodeManager;
import com.bushpath.anamnesis.namenode.NameSystem;
import com.bushpath.anamnesis.namenode.NameSystemFile;
import com.bushpath.anamnesis.protocol.ClientNamenodeProtocol;
import com.bushpath.anamnesis.protocol.Hdfs;

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
        }
    
        // convert NameSystemFile to HdfsFileStatusProto
        List<HdfsProtos.HdfsFileStatusProto> list = new ArrayList<>();
        for (NameSystemFile file: files) {
            HdfsProtos.FsPermissionProto permission = 
                Hdfs.buildFsPermissionProto(file.perm);

            HdfsProtos.HdfsFileStatusProto.FileType fileType = 
                file.file ? HdfsProtos.HdfsFileStatusProto.FileType.IS_FILE : 
                HdfsProtos.HdfsFileStatusProto.FileType.IS_DIR;

            HdfsProtos.HdfsFileStatusProto fileProto = Hdfs.buildHdfsFileStatusProto(
                fileType, new byte[]{}, file.length, permission, file.owner, file.group, 
                file.modificationTime, file.accessTime);

            list.add(fileProto);
        }


        HdfsProtos.DirectoryListingProto directoryListingProto = 
            Hdfs.buildDirectoryListingProto(list, 0);

        // respond to request
        ClientNamenodeProtocolProtos.GetListingResponseProto response =
            ClientNamenodeProtocol.buildGetListingResponseProto(directoryListingProto);

        responseObserver.onNext(response);
        responseObserver.onCompleted();
    }

    @Override
    public void mkdirs(ClientNamenodeProtocolProtos.MkdirsRequestProto req,
            StreamObserver<ClientNamenodeProtocolProtos.MkdirsResponseProto>
            responseObserver) {
        logger.info("recv create directory request");

        // use name system to make directory
        boolean success = true;
        try {
            this.nameSystem.mkdir(req.getSrc(), req.getMasked().getPerm(),
                req.getCreateParent());
        } catch (Exception e) {
            logger.severe(e.toString());
            success = false;
        }

        // respond to request
        ClientNamenodeProtocolProtos.MkdirsResponseProto response =
            ClientNamenodeProtocol.buildMkdirsResponseProto(success);

        responseObserver.onNext(response);
        responseObserver.onCompleted();
    }
}
