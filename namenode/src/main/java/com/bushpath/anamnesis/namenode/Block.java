package com.bushpath.anamnesis.namenode;

import com.google.protobuf.ByteString;
import org.apache.hadoop.hdfs.protocol.proto.HdfsProtos;
import org.apache.hadoop.security.proto.SecurityProtos;

import java.util.ArrayList;
import java.util.List;

public class Block {
    private long blockId, generationStamp, offset;

    private long length;
    private List<Datanode> locs;
    private List<Boolean> isCached;
    private List<HdfsProtos.StorageTypeProto> storageTypes;
    private List<String> storageIds;

    public Block(long blockId, long generationStamp, long offset) {
        this.blockId = blockId;
        this.generationStamp = generationStamp;
        this.offset = offset;

        this.length = 0;
        this.locs = new ArrayList<>();
        this.isCached = new ArrayList<>();
        this.storageTypes = new ArrayList<>();
        this.storageIds = new ArrayList<>();
    }

    public long getBlockId() {
        return this.blockId;
    }

    public long getGenerationStamp() {
        return this.generationStamp;
    }

    public long getOffset() {
        return this.offset;
    }

    public long getLength() {
        return this.length;
    }

    public void setLength(long length) {
        this.length = length;
    }

    public void addLoc(Datanode loc, boolean isCached,
            HdfsProtos.StorageTypeProto storageType, String storageId) {
        // check if location already exists
        for (Datanode datanode: this.locs) {
            if (datanode.getDatanodeUuid().equals(loc.getDatanodeUuid())) {
                return;
            }
        }

        this.locs.add(loc);
        this.isCached.add(isCached);
        this.storageTypes.add(storageType);
        this.storageIds.add(storageId);
    }

    public HdfsProtos.ExtendedBlockProto toExtendedBlockProto() {
        return HdfsProtos.ExtendedBlockProto.newBuilder()
            .setPoolId("")
            .setBlockId(this.blockId)
            .setGenerationStamp(this.generationStamp)
            .setNumBytes(this.length)
            .build();
    }

    public SecurityProtos.TokenProto toTokenProto() {
        return SecurityProtos.TokenProto.newBuilder()
            .setIdentifier(ByteString.copyFrom(new byte[]{}))
            .setPassword(ByteString.copyFrom(new byte[]{}))
            .setKind("")
            .setService("")
            .build();
    }

    public HdfsProtos.LocatedBlockProto toLocatedBlockProto() {
        List<HdfsProtos.DatanodeInfoProto> locs = new ArrayList<>();
        for (Datanode datanode: this.locs) {
            locs.add(datanode.toDatanodeInfoProto());
        }

        return HdfsProtos.LocatedBlockProto.newBuilder()
            .setB(this.toExtendedBlockProto())
            .setOffset(this.offset)
            .addAllLocs(locs)
            .setCorrupt(false)
            .setBlockToken(this.toTokenProto())
            .addAllIsCached(this.isCached)
            .addAllStorageTypes(this.storageTypes)
            .addAllStorageIDs(this.storageIds)
            .build();
    }
}
