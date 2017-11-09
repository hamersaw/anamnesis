package com.bushpath.anamnesis.namenode.rpc;

import com.google.protobuf.Message;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos;
import org.apache.hadoop.hdfs.protocol.proto.HdfsProtos;

import com.bushpath.anamnesis.namenode.Block;
import com.bushpath.anamnesis.namenode.BlockManager;
import com.bushpath.anamnesis.namenode.Configuration;
import com.bushpath.anamnesis.namenode.NameSystem;
import com.bushpath.anamnesis.namenode.NSFile;
import com.bushpath.anamnesis.namenode.NSItem;
import com.bushpath.anamnesis.util.Checksum;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

public class ClientNamenodeService {
    private NameSystem nameSystem;
    private BlockManager blockManager;
    private Configuration config;

    public ClientNamenodeService(NameSystem nameSystem,
            BlockManager blockManager, Configuration config) {
        this.nameSystem = nameSystem;
        this.blockManager = blockManager;
        this.config = config;
    }

    public Message addBlock(byte[] message) throws Exception {
        ClientNamenodeProtocolProtos.AddBlockRequestProto req =
            ClientNamenodeProtocolProtos.AddBlockRequestProto.parseFrom(message);

        // create block
        Block block = this.blockManager.createBlock(req.getSrc(), 
            req.getFavoredNodesList());

        // respond to request
        return ClientNamenodeProtocolProtos.AddBlockResponseProto.newBuilder()
            .setBlock(block.toLocatedBlockProto())
            .build();
    }

    public Message complete(byte[] message) throws Exception {
        ClientNamenodeProtocolProtos.CompleteRequestProto req =
            ClientNamenodeProtocolProtos.CompleteRequestProto.parseFrom(message);

        // complete file with name system
        this.nameSystem.complete(req.getSrc());

        // response to request
        return ClientNamenodeProtocolProtos.CompleteResponseProto.newBuilder()
            .setResult(true)
            .build();
    }

    public Message create(byte[] message) throws Exception {
        ClientNamenodeProtocolProtos.CreateRequestProto req =
            ClientNamenodeProtocolProtos.CreateRequestProto.parseFrom(message);

        // create file with name system
        NSItem item = this.nameSystem.create(req.getSrc(), req.getMasked().getPerm(),
            req.getClientName(), req.getCreateParent(), req.getBlockSize());

        // respond to request
        return ClientNamenodeProtocolProtos.CreateResponseProto.newBuilder()
            .setFs(item.toHdfsFileStatusProto(false))
            .build();
    }

    public Message getBlockLocations(byte[] message) throws Exception {
        ClientNamenodeProtocolProtos.GetBlockLocationsRequestProto req =
            ClientNamenodeProtocolProtos.GetBlockLocationsRequestProto.parseFrom(message);

        // look up file
        NSItem item = this.nameSystem.getFile(req.getSrc());
        if (item.getType() != NSItem.Type.FILE) {
            throw new Exception("file is not of type 'FILE'");
        }
        NSFile file = (NSFile) item;

        // return file locations
        return ClientNamenodeProtocolProtos.GetBlockLocationsResponseProto.newBuilder()
            .setLocations(file.toLocatedBlocksProto())
            .build();
    }

    public Message getFileInfo(byte[] message) throws Exception {
        ClientNamenodeProtocolProtos.GetFileInfoRequestProto req =
            ClientNamenodeProtocolProtos.GetFileInfoRequestProto.parseFrom(message);

        // query namesystem for file
        NSItem item = this.nameSystem.getFile(req.getSrc());
        ClientNamenodeProtocolProtos.GetFileInfoResponseProto.Builder respBuilder =
            ClientNamenodeProtocolProtos.GetFileInfoResponseProto.newBuilder();

        if (item != null) {
            respBuilder.setFs(item.toHdfsFileStatusProto(false));
        }
        return respBuilder.build();
    }

    public Message getListing(byte[] message) throws Exception {
        ClientNamenodeProtocolProtos.GetListingRequestProto req =
            ClientNamenodeProtocolProtos.GetListingRequestProto.parseFrom(message);

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
                item.toHdfsFileStatusProto(false);

            totalSize += hdfsFileStatusProto.getSerializedSize();
            if (totalSize > 110) {
                // if this will make proto larger than 127 (max 1 byte value)
                // TODO - figure out ideal value for this parameter
                //  using 110 to give 17 bytes for dir listings proto metadata
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

        // respond to request
        return ClientNamenodeProtocolProtos.GetListingResponseProto.newBuilder()
            .setDirList(directoryListingProtoBuilder.build())
            .build();
    }

    public Message getServerDefaults(byte[] message) throws Exception {
        ClientNamenodeProtocolProtos.GetServerDefaultsRequestProto req =
            ClientNamenodeProtocolProtos.GetServerDefaultsRequestProto.parseFrom(message);

        // retrieve server defaults
        HdfsProtos.FsServerDefaultsProto fsServerDefaultsProto =
            HdfsProtos.FsServerDefaultsProto.newBuilder()
                .setBlockSize(this.config.blockSize)
                .setBytesPerChecksum(Checksum.getBytesPerChecksum())
                .setWritePacketSize(this.config.writePacketSize)
                .setReplication(this.config.replication)
                .setFileBufferSize(this.config.fileBufferSize)
                .build();

        return ClientNamenodeProtocolProtos.GetServerDefaultsResponseProto.newBuilder()
            .setServerDefaults(fsServerDefaultsProto)
            .build();
    }

    public Message mkdirs(byte[] message) throws Exception {
        ClientNamenodeProtocolProtos.MkdirsRequestProto req =
            ClientNamenodeProtocolProtos.MkdirsRequestProto.parseFrom(message);

        // use name system to make directory
        this.nameSystem.mkdir(req.getSrc(), req.getMasked().getPerm(),
            req.getCreateParent());

        // respond to request
        return ClientNamenodeProtocolProtos.MkdirsResponseProto.newBuilder()
            .setResult(true)
            .build();
    }

    public Message rename(byte[] message) throws Exception {
        ClientNamenodeProtocolProtos.RenameRequestProto req =
            ClientNamenodeProtocolProtos.RenameRequestProto.parseFrom(message);

        // use name system to make directory
        this.nameSystem.rename(req.getSrc(), req.getDst());

        // respond to request
        return ClientNamenodeProtocolProtos.RenameResponseProto.newBuilder()
            .setResult(true)
            .build();
    }
}
    /*implements ClientNamenodeProtocolProtos.ClientNamenodeProtocol.BlockingInterface {
    
        public ClientNamenodeProtocolProtos.GetBlockLocationsResponseProto 
            getBlockLocations(com.google.protobuf.RpcController controller,
            ClientNamenodeProtocolProtos.GetBlockLocationsRequestProto request)
            throws com.google.protobuf.ServiceException {

            return null;
        }

        public ClientNamenodeProtocolProtos.GetServerDefaultsResponseProto 
            getServerDefaults(com.google.protobuf.RpcController controller,
            ClientNamenodeProtocolProtos.GetServerDefaultsRequestProto request)
            throws com.google.protobuf.ServiceException {

            return null;
        }

        public ClientNamenodeProtocolProtos.CreateResponseProto
            create(com.google.protobuf.RpcController controller,
            ClientNamenodeProtocolProtos.CreateRequestProto request)
            throws com.google.protobuf.ServiceException {

            return null;
        }

      public ClientNamenodeProtocolProtos.AppendResponseProto append(
          com.google.protobuf.RpcController controller,
          ClientNamenodeProtocolProtos.AppendRequestProto request)
          throws com.google.protobuf.ServiceException;

      public ClientNamenodeProtocolProtos.SetReplicationResponseProto setReplication(
          com.google.protobuf.RpcController controller,
          ClientNamenodeProtocolProtos.SetReplicationRequestProto request)
          throws com.google.protobuf.ServiceException;

      public ClientNamenodeProtocolProtos.SetStoragePolicyResponseProto setStoragePolicy(
          com.google.protobuf.RpcController controller,
          ClientNamenodeProtocolProtos.SetStoragePolicyRequestProto request)
          throws com.google.protobuf.ServiceException;

      public ClientNamenodeProtocolProtos.UnsetStoragePolicyResponseProto unsetStoragePolicy(
          com.google.protobuf.RpcController controller,
          ClientNamenodeProtocolProtos.UnsetStoragePolicyRequestProto request)
          throws com.google.protobuf.ServiceException;

      public ClientNamenodeProtocolProtos.GetStoragePolicyResponseProto getStoragePolicy(
          com.google.protobuf.RpcController controller,
          ClientNamenodeProtocolProtos.GetStoragePolicyRequestProto request)
          throws com.google.protobuf.ServiceException;

      public ClientNamenodeProtocolProtos.GetStoragePoliciesResponseProto getStoragePolicies(
          com.google.protobuf.RpcController controller,
          ClientNamenodeProtocolProtos.GetStoragePoliciesRequestProto request)
          throws com.google.protobuf.ServiceException;

      public ClientNamenodeProtocolProtos.SetPermissionResponseProto setPermission(
          com.google.protobuf.RpcController controller,
          ClientNamenodeProtocolProtos.SetPermissionRequestProto request)
          throws com.google.protobuf.ServiceException;

      public ClientNamenodeProtocolProtos.SetOwnerResponseProto setOwner(
          com.google.protobuf.RpcController controller,
          ClientNamenodeProtocolProtos.SetOwnerRequestProto request)
          throws com.google.protobuf.ServiceException;

      public ClientNamenodeProtocolProtos.AbandonBlockResponseProto abandonBlock(
          com.google.protobuf.RpcController controller,
          ClientNamenodeProtocolProtos.AbandonBlockRequestProto request)
          throws com.google.protobuf.ServiceException;

      public ClientNamenodeProtocolProtos.AddBlockResponseProto addBlock(
          com.google.protobuf.RpcController controller,
          ClientNamenodeProtocolProtos.AddBlockRequestProto request)
          throws com.google.protobuf.ServiceException;

      public ClientNamenodeProtocolProtos.GetAdditionalDatanodeResponseProto getAdditionalDatanode(
          com.google.protobuf.RpcController controller,
          ClientNamenodeProtocolProtos.GetAdditionalDatanodeRequestProto request)
          throws com.google.protobuf.ServiceException;

      public ClientNamenodeProtocolProtos.CompleteResponseProto complete(
          com.google.protobuf.RpcController controller,
          ClientNamenodeProtocolProtos.CompleteRequestProto request)
          throws com.google.protobuf.ServiceException;

      public ClientNamenodeProtocolProtos.ReportBadBlocksResponseProto reportBadBlocks(
          com.google.protobuf.RpcController controller,
          ClientNamenodeProtocolProtos.ReportBadBlocksRequestProto request)
          throws com.google.protobuf.ServiceException;

      public ClientNamenodeProtocolProtos.ConcatResponseProto concat(
          com.google.protobuf.RpcController controller,
          ClientNamenodeProtocolProtos.ConcatRequestProto request)
          throws com.google.protobuf.ServiceException;

      public ClientNamenodeProtocolProtos.TruncateResponseProto truncate(
          com.google.protobuf.RpcController controller,
          ClientNamenodeProtocolProtos.TruncateRequestProto request)
          throws com.google.protobuf.ServiceException;

      public ClientNamenodeProtocolProtos.RenameResponseProto rename(
          com.google.protobuf.RpcController controller,
          ClientNamenodeProtocolProtos.RenameRequestProto request)
          throws com.google.protobuf.ServiceException;

      public ClientNamenodeProtocolProtos.Rename2ResponseProto rename2(
          com.google.protobuf.RpcController controller,
          ClientNamenodeProtocolProtos.Rename2RequestProto request)
          throws com.google.protobuf.ServiceException;

      public ClientNamenodeProtocolProtos.DeleteResponseProto delete(
          com.google.protobuf.RpcController controller,
          ClientNamenodeProtocolProtos.DeleteRequestProto request)
          throws com.google.protobuf.ServiceException;

      public ClientNamenodeProtocolProtos.MkdirsResponseProto mkdirs(
          com.google.protobuf.RpcController controller,
          ClientNamenodeProtocolProtos.MkdirsRequestProto request)
          throws com.google.protobuf.ServiceException;

      public ClientNamenodeProtocolProtos.GetListingResponseProto getListing(
          com.google.protobuf.RpcController controller,
          ClientNamenodeProtocolProtos.GetListingRequestProto request)
          throws com.google.protobuf.ServiceException;

      public ClientNamenodeProtocolProtos.RenewLeaseResponseProto renewLease(
          com.google.protobuf.RpcController controller,
          ClientNamenodeProtocolProtos.RenewLeaseRequestProto request)
          throws com.google.protobuf.ServiceException;

      public ClientNamenodeProtocolProtos.RecoverLeaseResponseProto recoverLease(
          com.google.protobuf.RpcController controller,
          ClientNamenodeProtocolProtos.RecoverLeaseRequestProto request)
          throws com.google.protobuf.ServiceException;

      public ClientNamenodeProtocolProtos.GetFsStatsResponseProto getFsStats(
          com.google.protobuf.RpcController controller,
          ClientNamenodeProtocolProtos.GetFsStatusRequestProto request)
          throws com.google.protobuf.ServiceException;

      public ClientNamenodeProtocolProtos.GetDatanodeReportResponseProto getDatanodeReport(
          com.google.protobuf.RpcController controller,
          ClientNamenodeProtocolProtos.GetDatanodeReportRequestProto request)
          throws com.google.protobuf.ServiceException;

      public ClientNamenodeProtocolProtos.GetDatanodeStorageReportResponseProto getDatanodeStorageReport(
          com.google.protobuf.RpcController controller,
          ClientNamenodeProtocolProtos.GetDatanodeStorageReportRequestProto request)
          throws com.google.protobuf.ServiceException;

      public ClientNamenodeProtocolProtos.GetPreferredBlockSizeResponseProto getPreferredBlockSize(
          com.google.protobuf.RpcController controller,
          ClientNamenodeProtocolProtos.GetPreferredBlockSizeRequestProto request)
          throws com.google.protobuf.ServiceException;

      public ClientNamenodeProtocolProtos.SetSafeModeResponseProto setSafeMode(
          com.google.protobuf.RpcController controller,
          ClientNamenodeProtocolProtos.SetSafeModeRequestProto request)
          throws com.google.protobuf.ServiceException;

      public ClientNamenodeProtocolProtos.SaveNamespaceResponseProto saveNamespace(
          com.google.protobuf.RpcController controller,
          ClientNamenodeProtocolProtos.SaveNamespaceRequestProto request)
          throws com.google.protobuf.ServiceException;

      public ClientNamenodeProtocolProtos.RollEditsResponseProto rollEdits(
          com.google.protobuf.RpcController controller,
          ClientNamenodeProtocolProtos.RollEditsRequestProto request)
          throws com.google.protobuf.ServiceException;

      public ClientNamenodeProtocolProtos.RestoreFailedStorageResponseProto restoreFailedStorage(
          com.google.protobuf.RpcController controller,
          ClientNamenodeProtocolProtos.RestoreFailedStorageRequestProto request)
          throws com.google.protobuf.ServiceException;

      public ClientNamenodeProtocolProtos.RefreshNodesResponseProto refreshNodes(
          com.google.protobuf.RpcController controller,
          ClientNamenodeProtocolProtos.RefreshNodesRequestProto request)
          throws com.google.protobuf.ServiceException;

      public ClientNamenodeProtocolProtos.FinalizeUpgradeResponseProto finalizeUpgrade(
          com.google.protobuf.RpcController controller,
          ClientNamenodeProtocolProtos.FinalizeUpgradeRequestProto request)
          throws com.google.protobuf.ServiceException;

      public ClientNamenodeProtocolProtos.RollingUpgradeResponseProto rollingUpgrade(
          com.google.protobuf.RpcController controller,
          ClientNamenodeProtocolProtos.RollingUpgradeRequestProto request)
          throws com.google.protobuf.ServiceException;

      public ClientNamenodeProtocolProtos.ListCorruptFileBlocksResponseProto listCorruptFileBlocks(
          com.google.protobuf.RpcController controller,
          ClientNamenodeProtocolProtos.ListCorruptFileBlocksRequestProto request)
          throws com.google.protobuf.ServiceException;

      public ClientNamenodeProtocolProtos.MetaSaveResponseProto metaSave(
          com.google.protobuf.RpcController controller,
          ClientNamenodeProtocolProtos.MetaSaveRequestProto request)
          throws com.google.protobuf.ServiceException;

      public ClientNamenodeProtocolProtos.GetFileInfoResponseProto getFileInfo(
          com.google.protobuf.RpcController controller,
          ClientNamenodeProtocolProtos.GetFileInfoRequestProto request)
          throws com.google.protobuf.ServiceException;

      public ClientNamenodeProtocolProtos.AddCacheDirectiveResponseProto addCacheDirective(
          com.google.protobuf.RpcController controller,
          ClientNamenodeProtocolProtos.AddCacheDirectiveRequestProto request)
          throws com.google.protobuf.ServiceException;

      public ClientNamenodeProtocolProtos.ModifyCacheDirectiveResponseProto modifyCacheDirective(
          com.google.protobuf.RpcController controller,
          ClientNamenodeProtocolProtos.ModifyCacheDirectiveRequestProto request)
          throws com.google.protobuf.ServiceException;

      public ClientNamenodeProtocolProtos.RemoveCacheDirectiveResponseProto removeCacheDirective(
          com.google.protobuf.RpcController controller,
          ClientNamenodeProtocolProtos.RemoveCacheDirectiveRequestProto request)
          throws com.google.protobuf.ServiceException;

      public ClientNamenodeProtocolProtos.ListCacheDirectivesResponseProto listCacheDirectives(
          com.google.protobuf.RpcController controller,
          ClientNamenodeProtocolProtos.ListCacheDirectivesRequestProto request)
          throws com.google.protobuf.ServiceException;

      public ClientNamenodeProtocolProtos.AddCachePoolResponseProto addCachePool(
          com.google.protobuf.RpcController controller,
          ClientNamenodeProtocolProtos.AddCachePoolRequestProto request)
          throws com.google.protobuf.ServiceException;

      public ClientNamenodeProtocolProtos.ModifyCachePoolResponseProto modifyCachePool(
          com.google.protobuf.RpcController controller,
          ClientNamenodeProtocolProtos.ModifyCachePoolRequestProto request)
          throws com.google.protobuf.ServiceException;

      public ClientNamenodeProtocolProtos.RemoveCachePoolResponseProto removeCachePool(
          com.google.protobuf.RpcController controller,
          ClientNamenodeProtocolProtos.RemoveCachePoolRequestProto request)
          throws com.google.protobuf.ServiceException;

      public ClientNamenodeProtocolProtos.ListCachePoolsResponseProto listCachePools(
          com.google.protobuf.RpcController controller,
          ClientNamenodeProtocolProtos.ListCachePoolsRequestProto request)
          throws com.google.protobuf.ServiceException;

      public ClientNamenodeProtocolProtos.GetFileLinkInfoResponseProto getFileLinkInfo(
          com.google.protobuf.RpcController controller,
          ClientNamenodeProtocolProtos.GetFileLinkInfoRequestProto request)
          throws com.google.protobuf.ServiceException;

      public ClientNamenodeProtocolProtos.GetContentSummaryResponseProto getContentSummary(
          com.google.protobuf.RpcController controller,
          ClientNamenodeProtocolProtos.GetContentSummaryRequestProto request)
          throws com.google.protobuf.ServiceException;

      public ClientNamenodeProtocolProtos.SetQuotaResponseProto setQuota(
          com.google.protobuf.RpcController controller,
          ClientNamenodeProtocolProtos.SetQuotaRequestProto request)
          throws com.google.protobuf.ServiceException;

      public ClientNamenodeProtocolProtos.FsyncResponseProto fsync(
          com.google.protobuf.RpcController controller,
          ClientNamenodeProtocolProtos.FsyncRequestProto request)
          throws com.google.protobuf.ServiceException;

      public ClientNamenodeProtocolProtos.SetTimesResponseProto setTimes(
          com.google.protobuf.RpcController controller,
          ClientNamenodeProtocolProtos.SetTimesRequestProto request)
          throws com.google.protobuf.ServiceException;

      public ClientNamenodeProtocolProtos.CreateSymlinkResponseProto createSymlink(
          com.google.protobuf.RpcController controller,
          ClientNamenodeProtocolProtos.CreateSymlinkRequestProto request)
          throws com.google.protobuf.ServiceException;

      public ClientNamenodeProtocolProtos.GetLinkTargetResponseProto getLinkTarget(
          com.google.protobuf.RpcController controller,
          ClientNamenodeProtocolProtos.GetLinkTargetRequestProto request)
          throws com.google.protobuf.ServiceException;

      public ClientNamenodeProtocolProtos.UpdateBlockForPipelineResponseProto updateBlockForPipeline(
          com.google.protobuf.RpcController controller,
          ClientNamenodeProtocolProtos.UpdateBlockForPipelineRequestProto request)
          throws com.google.protobuf.ServiceException;

      public ClientNamenodeProtocolProtos.UpdatePipelineResponseProto updatePipeline(
          com.google.protobuf.RpcController controller,
          ClientNamenodeProtocolProtos.UpdatePipelineRequestProto request)
          throws com.google.protobuf.ServiceException;

      public org.apache.hadoop.security.proto.SecurityProtos.GetDelegationTokenResponseProto getDelegationToken(
          com.google.protobuf.RpcController controller,
          org.apache.hadoop.security.proto.SecurityProtos.GetDelegationTokenRequestProto request)
          throws com.google.protobuf.ServiceException;

      public org.apache.hadoop.security.proto.SecurityProtos.RenewDelegationTokenResponseProto renewDelegationToken(
          com.google.protobuf.RpcController controller,
          org.apache.hadoop.security.proto.SecurityProtos.RenewDelegationTokenRequestProto request)
          throws com.google.protobuf.ServiceException;

      public org.apache.hadoop.security.proto.SecurityProtos.CancelDelegationTokenResponseProto cancelDelegationToken(
          com.google.protobuf.RpcController controller,
          org.apache.hadoop.security.proto.SecurityProtos.CancelDelegationTokenRequestProto request)
          throws com.google.protobuf.ServiceException;

      public ClientNamenodeProtocolProtos.SetBalancerBandwidthResponseProto setBalancerBandwidth(
          com.google.protobuf.RpcController controller,
          ClientNamenodeProtocolProtos.SetBalancerBandwidthRequestProto request)
          throws com.google.protobuf.ServiceException;

      public ClientNamenodeProtocolProtos.GetDataEncryptionKeyResponseProto getDataEncryptionKey(
          com.google.protobuf.RpcController controller,
          ClientNamenodeProtocolProtos.GetDataEncryptionKeyRequestProto request)
          throws com.google.protobuf.ServiceException;

      public ClientNamenodeProtocolProtos.CreateSnapshotResponseProto createSnapshot(
          com.google.protobuf.RpcController controller,
          ClientNamenodeProtocolProtos.CreateSnapshotRequestProto request)
          throws com.google.protobuf.ServiceException;

      public ClientNamenodeProtocolProtos.RenameSnapshotResponseProto renameSnapshot(
          com.google.protobuf.RpcController controller,
          ClientNamenodeProtocolProtos.RenameSnapshotRequestProto request)
          throws com.google.protobuf.ServiceException;

      public ClientNamenodeProtocolProtos.AllowSnapshotResponseProto allowSnapshot(
          com.google.protobuf.RpcController controller,
          ClientNamenodeProtocolProtos.AllowSnapshotRequestProto request)
          throws com.google.protobuf.ServiceException;

      public ClientNamenodeProtocolProtos.DisallowSnapshotResponseProto disallowSnapshot(
          com.google.protobuf.RpcController controller,
          ClientNamenodeProtocolProtos.DisallowSnapshotRequestProto request)
          throws com.google.protobuf.ServiceException;

      public ClientNamenodeProtocolProtos.GetSnapshottableDirListingResponseProto getSnapshottableDirListing(
          com.google.protobuf.RpcController controller,
          ClientNamenodeProtocolProtos.GetSnapshottableDirListingRequestProto request)
          throws com.google.protobuf.ServiceException;

      public ClientNamenodeProtocolProtos.DeleteSnapshotResponseProto deleteSnapshot(
          com.google.protobuf.RpcController controller,
          ClientNamenodeProtocolProtos.DeleteSnapshotRequestProto request)
          throws com.google.protobuf.ServiceException;

      public ClientNamenodeProtocolProtos.GetSnapshotDiffReportResponseProto getSnapshotDiffReport(
          com.google.protobuf.RpcController controller,
          ClientNamenodeProtocolProtos.GetSnapshotDiffReportRequestProto request)
          throws com.google.protobuf.ServiceException;

      public ClientNamenodeProtocolProtos.IsFileClosedResponseProto isFileClosed(
          com.google.protobuf.RpcController controller,
          ClientNamenodeProtocolProtos.IsFileClosedRequestProto request)
          throws com.google.protobuf.ServiceException;

      public org.apache.hadoop.hdfs.protocol.proto.AclProtos.ModifyAclEntriesResponseProto modifyAclEntries(
          com.google.protobuf.RpcController controller,
          org.apache.hadoop.hdfs.protocol.proto.AclProtos.ModifyAclEntriesRequestProto request)
          throws com.google.protobuf.ServiceException;

      public org.apache.hadoop.hdfs.protocol.proto.AclProtos.RemoveAclEntriesResponseProto removeAclEntries(
          com.google.protobuf.RpcController controller,
          org.apache.hadoop.hdfs.protocol.proto.AclProtos.RemoveAclEntriesRequestProto request)
          throws com.google.protobuf.ServiceException;

      public org.apache.hadoop.hdfs.protocol.proto.AclProtos.RemoveDefaultAclResponseProto removeDefaultAcl(
          com.google.protobuf.RpcController controller,
          org.apache.hadoop.hdfs.protocol.proto.AclProtos.RemoveDefaultAclRequestProto request)
          throws com.google.protobuf.ServiceException;

      public org.apache.hadoop.hdfs.protocol.proto.AclProtos.RemoveAclResponseProto removeAcl(
          com.google.protobuf.RpcController controller,
          org.apache.hadoop.hdfs.protocol.proto.AclProtos.RemoveAclRequestProto request)
          throws com.google.protobuf.ServiceException;

      public org.apache.hadoop.hdfs.protocol.proto.AclProtos.SetAclResponseProto setAcl(
          com.google.protobuf.RpcController controller,
          org.apache.hadoop.hdfs.protocol.proto.AclProtos.SetAclRequestProto request)
          throws com.google.protobuf.ServiceException;

      public org.apache.hadoop.hdfs.protocol.proto.AclProtos.GetAclStatusResponseProto getAclStatus(
          com.google.protobuf.RpcController controller,
          org.apache.hadoop.hdfs.protocol.proto.AclProtos.GetAclStatusRequestProto request)
          throws com.google.protobuf.ServiceException;

      public org.apache.hadoop.hdfs.protocol.proto.XAttrProtos.SetXAttrResponseProto setXAttr(
          com.google.protobuf.RpcController controller,
          org.apache.hadoop.hdfs.protocol.proto.XAttrProtos.SetXAttrRequestProto request)
          throws com.google.protobuf.ServiceException;

      public org.apache.hadoop.hdfs.protocol.proto.XAttrProtos.GetXAttrsResponseProto getXAttrs(
          com.google.protobuf.RpcController controller,
          org.apache.hadoop.hdfs.protocol.proto.XAttrProtos.GetXAttrsRequestProto request)
          throws com.google.protobuf.ServiceException;

      public org.apache.hadoop.hdfs.protocol.proto.XAttrProtos.ListXAttrsResponseProto listXAttrs(
          com.google.protobuf.RpcController controller,
          org.apache.hadoop.hdfs.protocol.proto.XAttrProtos.ListXAttrsRequestProto request)
          throws com.google.protobuf.ServiceException;

      public org.apache.hadoop.hdfs.protocol.proto.XAttrProtos.RemoveXAttrResponseProto removeXAttr(
          com.google.protobuf.RpcController controller,
          org.apache.hadoop.hdfs.protocol.proto.XAttrProtos.RemoveXAttrRequestProto request)
          throws com.google.protobuf.ServiceException;

      public ClientNamenodeProtocolProtos.CheckAccessResponseProto checkAccess(
          com.google.protobuf.RpcController controller,
          ClientNamenodeProtocolProtos.CheckAccessRequestProto request)
          throws com.google.protobuf.ServiceException;

      public org.apache.hadoop.hdfs.protocol.proto.EncryptionZonesProtos.CreateEncryptionZoneResponseProto createEncryptionZone(
          com.google.protobuf.RpcController controller,
          org.apache.hadoop.hdfs.protocol.proto.EncryptionZonesProtos.CreateEncryptionZoneRequestProto request)
          throws com.google.protobuf.ServiceException;

      public org.apache.hadoop.hdfs.protocol.proto.EncryptionZonesProtos.ListEncryptionZonesResponseProto listEncryptionZones(
          com.google.protobuf.RpcController controller,
          org.apache.hadoop.hdfs.protocol.proto.EncryptionZonesProtos.ListEncryptionZonesRequestProto request)
          throws com.google.protobuf.ServiceException;

      public org.apache.hadoop.hdfs.protocol.proto.EncryptionZonesProtos.GetEZForPathResponseProto getEZForPath(
          com.google.protobuf.RpcController controller,
          org.apache.hadoop.hdfs.protocol.proto.EncryptionZonesProtos.GetEZForPathRequestProto request)
          throws com.google.protobuf.ServiceException;

      public ClientNamenodeProtocolProtos.GetCurrentEditLogTxidResponseProto getCurrentEditLogTxid(
          com.google.protobuf.RpcController controller,
          ClientNamenodeProtocolProtos.GetCurrentEditLogTxidRequestProto request)
          throws com.google.protobuf.ServiceException;

      public ClientNamenodeProtocolProtos.GetEditsFromTxidResponseProto getEditsFromTxid(
          com.google.protobuf.RpcController controller,
          ClientNamenodeProtocolProtos.GetEditsFromTxidRequestProto request)
          throws com.google.protobuf.ServiceException;

      public ClientNamenodeProtocolProtos.GetQuotaUsageResponseProto getQuotaUsage(
          com.google.protobuf.RpcController controller,
          ClientNamenodeProtocolProtos.GetQuotaUsageRequestProto request)
          throws com.google.protobuf.ServiceException;
}*/
