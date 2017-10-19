package com.bushpath.anamnesis.namenode;

import org.apache.hadoop.hdfs.protocol.proto.HdfsProtos;

import java.util.ArrayList;
import java.util.List;

public class Block {
    public long blockId, generationStamp, offset;
    public List<HdfsProtos.DatanodeInfoProto> locs;

    public Block(long blockId, long generationStamp, long offset) {
        this.blockId = blockId;
        this.generationStamp = generationStamp;
        this.offset = offset;
        this.locs = new ArrayList<>();
    }

    public void addLoc(HdfsProtos.DatanodeInfoProto loc) {
        this.locs.add(loc);
    }
}
