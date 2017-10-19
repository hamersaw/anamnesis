package com.bushpath.anamnesis.namenode;

import com.google.protobuf.ByteString;
import org.apache.hadoop.hdfs.protocol.proto.HdfsProtos;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

public class NSDirectory extends NSItem {
    private Map<String, NSItem> children;

    public NSDirectory(String name, int perm) {
        super(name, NSItem.Type.DIRECTORY, perm);
        this.children = new HashMap<>();
    }

    public boolean hasChild(String name) {
        return this.children.containsKey(name);
    }

    public NSItem getChild(String name) {
        return this.children.get(name);
    }

    public void addChild(NSItem child) {
        children.put(child.getName(), child);
    }

    public Collection<NSItem> getChildren() {
        return this.children.values();
    }

    @Override
    public HdfsProtos.HdfsFileStatusProto toHdfsFileStatusProto() {
        HdfsProtos.FsPermissionProto permission = 
            HdfsProtos.FsPermissionProto.newBuilder()
                .setPerm(this.perm)
                .build();

        return HdfsProtos.HdfsFileStatusProto.newBuilder()
            .setFileType(HdfsProtos.HdfsFileStatusProto.FileType.IS_DIR)
            .setPath(ByteString.copyFrom(this.name.getBytes()))
            .setLength(-1)
            .setPermission(permission)
            .setOwner("")
            .setGroup("")
            .setModificationTime(-1)
            .setAccessTime(-1)
            .build();
    }
}
