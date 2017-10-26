package com.bushpath.anamnesis.client;

import org.apache.hadoop.hdfs.protocol.proto.HdfsProtos;

import java.util.ArrayList;
import java.util.List;

public class Block {
    private long blockId, offset;
    private List<Location> locations;

    public Block(long blockId, long offset, List<Location> locations) {
        this.blockId = blockId;
        this.offset = offset;
        this.locations = locations;
    }

    public long getBlockId() {
        return this.blockId;
    }

    public long getOffset() {
        return this.offset;
    }

    public List<Location> getLocations() {
        return this.locations;
    }

    public static Block parseFrom(HdfsProtos.LocatedBlockProto locatedBlockProto) {
        long blockId = locatedBlockProto.getB().getBlockId();
        long offset = locatedBlockProto.getOffset();

        List<Location> locations = new ArrayList<>();
        for (HdfsProtos.DatanodeInfoProto datanodeInfoProto:
                locatedBlockProto.getLocsList()) {
            locations.add(Location.parseFrom(datanodeInfoProto.getId()));
        }

        return new Block(blockId, offset, locations);
    }
}
