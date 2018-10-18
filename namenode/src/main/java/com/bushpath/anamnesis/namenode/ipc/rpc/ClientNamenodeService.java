package com.bushpath.anamnesis.namenode.ipc.rpc;

import com.google.protobuf.Message;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos;
import org.apache.hadoop.hdfs.protocol.proto.HdfsProtos;

import com.bushpath.anamnesis.ipc.datatransfer.DataTransferProtocol;
import com.bushpath.anamnesis.ipc.rpc.SocketContext;
import com.bushpath.anamnesis.namenode.Block;
import com.bushpath.anamnesis.namenode.BlockManager;
import com.bushpath.anamnesis.namenode.Configuration;
import com.bushpath.anamnesis.namenode.Datanode;
import com.bushpath.anamnesis.namenode.DatanodeManager;
import com.bushpath.anamnesis.namenode.namesystem.NameSystem;
import com.bushpath.anamnesis.namenode.namesystem.NSFile;
import com.bushpath.anamnesis.namenode.namesystem.NSItem;

import java.io.DataInputStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Random;

public class ClientNamenodeService {
    private NameSystem nameSystem;
    private BlockManager blockManager;
    private DatanodeManager datanodeManager;
    private Configuration config;

    public ClientNamenodeService(NameSystem nameSystem,
            BlockManager blockManager, DatanodeManager datanodeManager,
            Configuration config) {
        this.nameSystem = nameSystem;
        this.blockManager = blockManager;
        this.datanodeManager = datanodeManager;
        this.config = config;
    }

    public Message addBlock(DataInputStream in,
            SocketContext socketContext) throws Exception {
        ClientNamenodeProtocolProtos.AddBlockRequestProto req =
            ClientNamenodeProtocolProtos.AddBlockRequestProto.parseDelimitedFrom(in);

        // retrieve file for block
        NSItem item = nameSystem.getFile(req.getSrc());
        if (item.getType() != NSItem.Type.FILE) {
            throw new Exception("file '" + req.getSrc() + "' is not of type 'FILE'");
        }
        NSFile file = (NSFile) item;

        // get location for block
        Datanode datanode = null;
        List<String> favoredNodes = req.getFavoredNodesList();
        if (favoredNodes != null && favoredNodes.size() != 0) {
            for (String favoredNode: favoredNodes) {
                if (datanodeManager.contains(favoredNode)) {
                    datanode = datanodeManager.get(favoredNode);
                    break;
                }
            }
        }

        if (datanode == null) {
            datanode = this.datanodeManager.getRandom();
        }

        // create block
        Random random = new Random();
        Block block = new Block(random.nextLong(), System.currentTimeMillis(), file);
        file.addBlock(block);
        this.blockManager.add(block);

        System.out.println(System.currentTimeMillis() + ": added block "
            + block.getBlockId() + " : '" + req.getSrc()
            + "' : " + datanode.getDatanodeUuid());

        // respond to request
        HdfsProtos.LocatedBlockProto locatedBlockProto =
            HdfsProtos.LocatedBlockProto.newBuilder()
                .setB(block.toExtendedBlockProto())
                .setOffset(block.getOffset())
                .addLocs(datanode.toDatanodeInfoProto())
                .setCorrupt(false)
                .setBlockToken(block.toTokenProto())
                .addIsCached(true)
                .addStorageTypes(HdfsProtos.StorageTypeProto.RAM_DISK)
                .addStorageIDs("")
                .build();

        return ClientNamenodeProtocolProtos.AddBlockResponseProto.newBuilder()
            .setBlock(locatedBlockProto)
            .build();
    }

    public Message complete(DataInputStream in,
            SocketContext socketContext) throws Exception {
        ClientNamenodeProtocolProtos.CompleteRequestProto req =
            ClientNamenodeProtocolProtos.CompleteRequestProto.parseDelimitedFrom(in);

        // complete file with name system
        this.nameSystem.complete(req.getSrc());

        System.out.println(System.currentTimeMillis() + ": completed file '"
            + req.getSrc() + "'");

        // response to request
        return ClientNamenodeProtocolProtos.CompleteResponseProto.newBuilder()
            .setResult(true)
            .build();
    }

    public Message create(DataInputStream in,
            SocketContext socketContext) throws Exception {
        ClientNamenodeProtocolProtos.CreateRequestProto req =
            ClientNamenodeProtocolProtos.CreateRequestProto.parseDelimitedFrom(in);

        // create file with name system
        NSItem item = this.nameSystem.create(req.getSrc(), req.getMasked().getPerm(),
            socketContext.getEffectiveUser(), socketContext.getEffectiveUser(),
            req.getCreateParent(), req.getBlockSize());

        System.out.println(System.currentTimeMillis() + ": created file '"
            + req.getSrc() + "' " + req.getCreateFlag());

        // respond to request
        return ClientNamenodeProtocolProtos.CreateResponseProto.newBuilder()
            .setFs(item.toHdfsFileStatusProto(false))
            .build();
    }

    public Message delete(DataInputStream in,
            SocketContext socketContext) throws Exception {
        ClientNamenodeProtocolProtos.DeleteRequestProto req =
            ClientNamenodeProtocolProtos.DeleteRequestProto.parseDelimitedFrom(in);

        // delete file from namesystem
        List<Long> blockIds = this.nameSystem.delete(req.getSrc(), req.getRecursive());
        for (Long blockId : blockIds) {
            this.blockManager.delete(blockId);
        }

        System.out.println(System.currentTimeMillis() + ": deleted file '"
            + req.getSrc() + "' : " + req.getRecursive());

        return ClientNamenodeProtocolProtos.DeleteResponseProto.newBuilder()
            .setResult(true)
            .build();
    }

    public Message fsync(DataInputStream in,
            SocketContext socketContext) throws Exception {
        ClientNamenodeProtocolProtos.FsyncRequestProto req =
            ClientNamenodeProtocolProtos.FsyncRequestProto.parseDelimitedFrom(in);

        System.out.println(System.currentTimeMillis() + ": fsync for file '"
            + req.getSrc() + "'");

        // return file locations
        return ClientNamenodeProtocolProtos.FsyncResponseProto.newBuilder()
            .build();
    }

    public Message getBlockLocations(DataInputStream in,
            SocketContext socketContext) throws Exception {
        ClientNamenodeProtocolProtos.GetBlockLocationsRequestProto req =
            ClientNamenodeProtocolProtos.GetBlockLocationsRequestProto.parseDelimitedFrom(in);

        // look up file
        NSItem item = this.nameSystem.getFile(req.getSrc());
        if (item.getType() != NSItem.Type.FILE) {
            throw new Exception("file is not of type 'FILE'");
        }
        NSFile file = (NSFile) item;

        System.out.println(System.currentTimeMillis() + ": got block locations for file '"
            + req.getSrc() + "' : " + ((NSFile) item).getBlockCount());

        // return file locations
        return ClientNamenodeProtocolProtos.GetBlockLocationsResponseProto.newBuilder()
            .setLocations(file.toLocatedBlocksProto())
            .build();
    }

    public Message getDatanodeStorageReport(DataInputStream in,
            SocketContext socketContext) throws Exception {
        ClientNamenodeProtocolProtos.GetDatanodeStorageReportRequestProto req =
            ClientNamenodeProtocolProtos.GetDatanodeStorageReportRequestProto
                .parseDelimitedFrom(in);

        // iterate over datanodes
        List<ClientNamenodeProtocolProtos.DatanodeStorageReportProto>
            datanodeStorageReports = new ArrayList<>();

        for (Datanode datanode : this.datanodeManager.getDatanodes()) {
            datanodeStorageReports.add(datanode.toDatanodeStorageReportProto());
        }

        // return datanode storage reports
        return ClientNamenodeProtocolProtos.GetDatanodeStorageReportResponseProto
            .newBuilder()
                .addAllDatanodeStorageReports(datanodeStorageReports)
                .build();
    }

    public Message getFileInfo(DataInputStream in,
            SocketContext socketContext) throws Exception {
        ClientNamenodeProtocolProtos.GetFileInfoRequestProto req =
            ClientNamenodeProtocolProtos.GetFileInfoRequestProto.parseDelimitedFrom(in);

        // query namesystem for file
        NSItem item = this.nameSystem.getFile(req.getSrc());
        ClientNamenodeProtocolProtos.GetFileInfoResponseProto.Builder respBuilder =
            ClientNamenodeProtocolProtos.GetFileInfoResponseProto.newBuilder();

        System.out.println(System.currentTimeMillis() + ": getFileInfo for file '"
            + req.getSrc() + "'");

        if (item != null) {
            respBuilder.setFs(item.toHdfsFileStatusProto(false));
        }
        return respBuilder.build();
    }

    public Message getListing(DataInputStream in,
            SocketContext socketContext) throws Exception {
        ClientNamenodeProtocolProtos.GetListingRequestProto req =
            ClientNamenodeProtocolProtos.GetListingRequestProto.parseDelimitedFrom(in);

        String startAfter = new String(req.getStartAfter().toByteArray());
        Collection<NSItem> items = this.nameSystem.getListing(req.getSrc());

        // get start index
        int startIndex = 0;
        if (!startAfter.isEmpty()) {
            for (NSItem item: items) {
                startIndex += 1;

                if (item.getPath().equals(startAfter)) {
                    break;
                }
            }
        }

        HdfsProtos.DirectoryListingProto.Builder directoryListingProtoBuilder = 
            HdfsProtos.DirectoryListingProto.newBuilder();

        int totalSize = 0, index = 0;
        for (NSItem item: items) {
            if (index < startIndex) {
                index += 1;
                continue;
            }

            // convert NSItem to HdfsFileStatusProto
            HdfsProtos.HdfsFileStatusProto hdfsFileStatusProto =
                item.toHdfsFileStatusProto(req.getNeedLocation());

            totalSize += hdfsFileStatusProto.getSerializedSize();
            if (totalSize > 60000) { // TODO - find the ideal size for this
                directoryListingProtoBuilder
                    .setRemainingEntries(items.size() - index);
                break;
            } else {
                directoryListingProtoBuilder.addPartialListing(hdfsFileStatusProto);
            }

            index += 1;
        }

        if (index >= items.size()) {
            directoryListingProtoBuilder.setRemainingEntries(0);
        }

        System.out.println(System.currentTimeMillis() + ": got listings for file '"
            + req.getSrc() + "'");

        // respond to request
        return ClientNamenodeProtocolProtos.GetListingResponseProto.newBuilder()
            .setDirList(directoryListingProtoBuilder.build())
            .build();
    }

    public Message getServerDefaults(DataInputStream in,
            SocketContext socketContext) throws Exception {
        ClientNamenodeProtocolProtos.GetServerDefaultsRequestProto req =
            ClientNamenodeProtocolProtos.GetServerDefaultsRequestProto.parseDelimitedFrom(in);

        // retrieve server defaults
        HdfsProtos.FsServerDefaultsProto fsServerDefaultsProto =
            HdfsProtos.FsServerDefaultsProto.newBuilder()
                .setBlockSize(this.config.blockSize)
                .setBytesPerChecksum(DataTransferProtocol.CHUNK_SIZE)
                .setWritePacketSize(this.config.writePacketSize)
                .setReplication(this.config.replication)
                .setFileBufferSize(this.config.fileBufferSize)
                .setChecksumType(HdfsProtos.ChecksumTypeProto.CHECKSUM_CRC32C)
                .build();

        //System.out.println(System.currentTimeMillis() + ": got server defaults");

        return ClientNamenodeProtocolProtos.GetServerDefaultsResponseProto.newBuilder()
            .setServerDefaults(fsServerDefaultsProto)
            .build();
    }

    public Message mkdirs(DataInputStream in,
            SocketContext socketContext) throws Exception {
        ClientNamenodeProtocolProtos.MkdirsRequestProto req =
            ClientNamenodeProtocolProtos.MkdirsRequestProto.parseDelimitedFrom(in);

        // use name system to make directory
        this.nameSystem.mkdir(req.getSrc(), req.getMasked().getPerm(),
            socketContext.getEffectiveUser(), socketContext.getEffectiveUser(),
            req.getCreateParent());

        System.out.println(System.currentTimeMillis() + ": made directory '"
            + req.getSrc() + "' : " + req.getCreateParent());

        // respond to request
        return ClientNamenodeProtocolProtos.MkdirsResponseProto.newBuilder()
            .setResult(true)
            .build();
    }

    public Message rename(DataInputStream in,
            SocketContext socketContext) throws Exception {
        ClientNamenodeProtocolProtos.RenameRequestProto req =
            ClientNamenodeProtocolProtos.RenameRequestProto.parseDelimitedFrom(in);

        // use name system to make directory
        this.nameSystem.rename(req.getSrc(), req.getDst());

        System.out.println(System.currentTimeMillis() + ": renamed '"
            + req.getSrc() + "' to '" + req.getDst() + "'");

        // respond to request
        return ClientNamenodeProtocolProtos.RenameResponseProto.newBuilder()
            .setResult(true)
            .build();
    }

    public Message renewLease(DataInputStream in,
            SocketContext socketContext) throws Exception {
        ClientNamenodeProtocolProtos.RenewLeaseRequestProto req =
            ClientNamenodeProtocolProtos.RenewLeaseRequestProto.parseDelimitedFrom(in);

        // response to request
        return ClientNamenodeProtocolProtos.RenewLeaseResponseProto.newBuilder()
            .build();
    }

    public Message setOwner(DataInputStream in,
            SocketContext socketContext) throws Exception {
        ClientNamenodeProtocolProtos.SetOwnerRequestProto req =
            ClientNamenodeProtocolProtos.SetOwnerRequestProto.parseDelimitedFrom(in);

        // retrieve item
        NSItem item = this.nameSystem.getFile(req.getSrc());
        if (req.hasUsername()) {
            item.setOwner(req.getUsername());
        }

        if (req.hasGroupname()) {
            item.setGroup(req.getGroupname());
        }

        System.out.println(System.currentTimeMillis() + ": changed owner of '"
            + req.getSrc()  + "' to '" + req.getUsername() + ":"
            + req.getGroupname() + "'");

        // response to request
        return ClientNamenodeProtocolProtos.SetPermissionResponseProto.newBuilder()
            .build();
    }

    public Message setPermission(DataInputStream in,
            SocketContext socketContext) throws Exception {
        ClientNamenodeProtocolProtos.SetPermissionRequestProto req =
            ClientNamenodeProtocolProtos.SetPermissionRequestProto.parseDelimitedFrom(in);

        // retrieve item
        NSItem item = this.nameSystem.getFile(req.getSrc());
        item.setPerm(req.getPermission().getPerm());

        System.out.println(System.currentTimeMillis() + ": set permission of '"
            + req.getSrc() + "' to " + req.getPermission().getPerm());

        // response to request
        return ClientNamenodeProtocolProtos.SetPermissionResponseProto.newBuilder()
            .build();
    }

    public Message setReplication(DataInputStream in,
            SocketContext socketContext) throws Exception {
        ClientNamenodeProtocolProtos.SetReplicationRequestProto req =
            ClientNamenodeProtocolProtos.SetReplicationRequestProto.parseDelimitedFrom(in);

        // dummy method returning success - we don't currently support replication

        // response to request
        return ClientNamenodeProtocolProtos.SetReplicationResponseProto.newBuilder()
            .setResult(true)
            .build();
    }
}
