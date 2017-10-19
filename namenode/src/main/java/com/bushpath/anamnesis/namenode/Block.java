package com.bushpath.anamnesis.namenode;

import com.google.protobuf.ByteString;
import org.apache.hadoop.hdfs.protocol.proto.HdfsProtos;
import org.apache.hadoop.security.proto.SecurityProtos;

import java.util.ArrayList;
import java.util.List;

public class Block {
    public long blockId, generationStamp, offset;
    public List<HdfsProtos.DatanodeInfoProto> locs;
    public List<Boolean> isCached;
    public List<HdfsProtos.StorageTypeProto> storageTypes;
    public List<String> storageIds;

    public Block(long blockId, long generationStamp, long offset) {
        this.blockId = blockId;
        this.generationStamp = generationStamp;
        this.offset = offset;
        this.locs = new ArrayList<>();
        this.isCached = new ArrayList<>();
        this.storageTypes = new ArrayList<>();
        this.storageIds = new ArrayList<>();
    }

    public void addLoc(HdfsProtos.DatanodeInfoProto loc) {
        this.locs.add(loc);
    }

    public HdfsProtos.LocatedBlockProto toLocatedBlockProto() {
        HdfsProtos.ExtendedBlockProto b = HdfsProtos.ExtendedBlockProto.newBuilder()
            .setPoolId("")
            .setBlockId(this.blockId)
            .setGenerationStamp(this.generationStamp)
            .build();

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
