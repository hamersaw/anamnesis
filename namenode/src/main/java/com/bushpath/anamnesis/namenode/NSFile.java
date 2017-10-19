package com.bushpath.anamnesis.namenode;

import com.google.protobuf.ByteString;
import org.apache.hadoop.hdfs.protocol.proto.HdfsProtos;

import java.util.ArrayList;
import java.util.List;

public class NSFile extends NSItem {
    private String owner, group;
    private long modificationTime, accessTime, length, blockSize;
    private List<Long> blocks;
    private boolean complete;

    public NSFile (String name, String owner, String group, int perm, long blockSize) {
        super(name, NSItem.Type.FILE, perm);

        this.owner = owner;
        this.group = group;
        this.modificationTime = System.currentTimeMillis();
        this.accessTime = this.modificationTime;
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

    public long getModificationTime() {
        return this.modificationTime;
    }

    public void setModificationTime(long modificationTime) {
        this.modificationTime = modificationTime;
    }

    public long getAccessTime() {
        return this.accessTime;
    }

    public void setAccessTime(long accessTime) {
        this.accessTime = accessTime;
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

    public List<Long> getBlocks() {
        return this.blocks;
    }

    public int getBlockCount() {
        return this.blocks.size();
    }

    public void addBlock(long blockId) {
        this.blocks.add(blockId);
    }

    public boolean isComplete() {
        return this.complete;
    }

    public void complete() {
        this.complete = true;
    }

    @Override
    public HdfsProtos.HdfsFileStatusProto toHdfsFileStatusProto() {
        HdfsProtos.FsPermissionProto permission = 
            HdfsProtos.FsPermissionProto.newBuilder()
                .setPerm(this.perm)
                .build();

        return HdfsProtos.HdfsFileStatusProto.newBuilder()
            .setFileType(HdfsProtos.HdfsFileStatusProto.FileType.IS_FILE)
            .setPath(ByteString.copyFrom(this.name.getBytes()))
            .setLength(this.length)
            .setPermission(permission)
            .setOwner(this.owner)
            .setGroup(this.group)
            .setModificationTime(this.modificationTime)
            .setAccessTime(this.accessTime)
            .build();
    }
}
