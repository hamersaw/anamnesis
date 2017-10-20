package com.bushpath.anamnesis.namenode;

import com.google.protobuf.ByteString;
import org.apache.hadoop.hdfs.protocol.proto.HdfsProtos;
import org.apache.hadoop.security.proto.SecurityProtos;

import java.util.ArrayList;
import java.util.List;

public class Block {
    private long blockId, generationStamp, offset;
    private List<Datanode> locs;
    private List<Boolean> isCached;
    private List<HdfsProtos.StorageTypeProto> storageTypes;
    private List<String> storageIds;

    public Block(long blockId, long generationStamp, long offset) {
        this.blockId = blockId;
        this.generationStamp = generationStamp;
        this.offset = offset;
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
        return this.getOffset();
    }

    public void addLoc(Datanode loc) {
        this.locs.add(loc);
    }

    public HdfsProtos.LocatedBlockProto toLocatedBlockProto() {
        HdfsProtos.ExtendedBlockProto b = HdfsProtos.ExtendedBlockProto.newBuilder()
            .setPoolId("")
            .setBlockId(this.blockId)
            .setGenerationStamp(this.generationStamp)
            .build();

        List<HdfsProtos.DatanodeInfoProto> locs = new ArrayList<>();
        for (Datanode datanode: this.locs) {
            locs.add(datanode.toDatanodeInfoProto());
        }

        SecurityProtos.TokenProto blockToken = SecurityProtos.TokenProto.newBuilder()
            .setIdentifier(ByteString.copyFrom(new byte[]{}))
            .setPassword(ByteString.copyFrom(new byte[]{}))
            .setKind("")
            .setService("")
            .build();

        return HdfsProtos.LocatedBlockProto.newBuilder()
            .setB(b)
            .setOffset(this.offset)
            .addAllLocs(locs)
            .setCorrupt(false)
            .setBlockToken(blockToken)
            .addAllIsCached(this.isCached)
            .addAllStorageTypes(this.storageTypes)
            .addAllStorageIDs(this.storageIds)
            .build();
    }
}
