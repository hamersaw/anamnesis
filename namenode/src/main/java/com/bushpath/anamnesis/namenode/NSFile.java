package com.bushpath.anamnesis.namenode;

import com.google.protobuf.ByteString;
import org.apache.hadoop.hdfs.protocol.proto.HdfsProtos;

import java.util.ArrayList;
import java.util.List;

public class NSFile extends NSItem {
    private String owner, group;
    private long modificationTime, accessTime, length, blockSize;
    private List<Block> blocks;
    private boolean complete;

    public NSFile (String name, String owner, String group, int perm,
            long blockSize, NSItem parent) {
        super(name, NSItem.Type.FILE, perm, parent);

        this.owner = owner;
        this.group = group;
        this.length = 0;
        this.blockSize = blockSize;
        this.blocks = new ArrayList<>();
        this.complete = false;
    }

    public String getOwner() {
        return this.owner;
    }

    public String getGroup() {
        return this.group;
    }

    public long getLength() {
        return this.length;
    }

    public void incLength(long delta) {
        this.length += delta;
    }

    public long getBlockSize() {
        return this.blockSize;
    }

    public List<Block> getBlocks() {
        return this.blocks;
    }

    public int getBlockCount() {
        return this.blocks.size();
    }

    public void addBlock(Block block) {
        this.blocks.add(block);
    }

    public boolean isComplete() {
        return this.complete;
    }

    public void complete() {
        this.complete = true;
    }

    public HdfsProtos.LocatedBlocksProto toLocatedBlocksProto() {
        List<HdfsProtos.LocatedBlockProto> blocks = new ArrayList<>();
        for (Block block: this.blocks) {
            blocks.add(block.toLocatedBlockProto());
        }

        return HdfsProtos.LocatedBlocksProto.newBuilder()
            .setFileLength(this.length)
            .addAllBlocks(blocks)
            .setUnderConstruction(!this.complete)
            .setIsLastBlockComplete(false) // TODO
            .build();
    }

    @Override
    public HdfsProtos.HdfsFileStatusProto toHdfsFileStatusProto(boolean needLocation) {
        HdfsProtos.FsPermissionProto permission = 
            HdfsProtos.FsPermissionProto.newBuilder()
                .setPerm(this.perm)
                .build();

        HdfsProtos.HdfsFileStatusProto.Builder builder 
            = HdfsProtos.HdfsFileStatusProto.newBuilder()
                .setFileType(HdfsProtos.HdfsFileStatusProto.FileType.IS_FILE)
                .setPath(ByteString.copyFrom(this.getPath().getBytes()))
                .setLength(this.length)
                .setPermission(permission)
                .setOwner(this.owner)
                .setGroup(this.group)
                .setModificationTime(this.modificationTime)
                .setAccessTime(this.accessTime)
                .setBlocksize(this.blockSize);

        if (needLocation) {
            builder.setLocations(this.toLocatedBlocksProto());
        }

        return builder.build();
    }

    @Override
    public void print(int indent) {
        for (int i=0; i<indent; i++) {
            System.out.print("\t");
        }

        System.out.println("FILE:" + this.name);
    }
}
